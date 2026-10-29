package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

const (
	uiSecretHeader      = "X-Bridge-UI-Secret"
	sendSecretHeader    = "X-Bridge-Send-Secret"
	maxRequestBodyBytes = 1 << 20

	loginStepDeadline   = 55 * time.Second
	actionDeadline      = 14 * time.Second
	sendTextDeadline    = 14 * time.Second
	sendMediaDeadline   = 115 * time.Second
	typingDeadline      = 2500 * time.Millisecond
	httpReadTimeout     = 30 * time.Second
	httpWriteTimeout    = 130 * time.Second
	httpIdleTimeout     = 120 * time.Second
	httpShutdownTimeout = 10 * time.Second
)

type HTTPAPI struct {
	config  *Config
	slots   *SlotManager
	logins  *LoginManager
	media   *MediaStore
	log     zerolog.Logger
	version string
}

func (api *HTTPAPI) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", api.handleRoot)

	ui := func(handler http.HandlerFunc) http.Handler {
		return api.requireSecret(uiSecretHeader, api.config.UISecret, handler)
	}
	send := func(handler http.HandlerFunc) http.Handler {
		return api.requireSecret(sendSecretHeader, api.config.SendSecret, handler)
	}

	mux.Handle("POST /auth/login/start", ui(api.handleLoginStart))
	mux.Handle("POST /auth/login/submit", ui(api.handleLoginSubmit))
	mux.Handle("POST /auth/login/cancel", ui(api.handleLoginCancel))
	mux.Handle("POST /auth/logout", ui(api.handleLogout))
	mux.Handle("GET /health", ui(api.handleHealth))
	mux.Handle("GET /media/{token}", ui(api.handleMedia))
	mux.Handle("GET /chats/members", ui(api.handleChatMembers))

	mux.Handle("POST /messages/send", send(api.handleSend))
	mux.Handle("POST /messages/reaction", send(api.handleReaction))
	mux.Handle("POST /messages/unsend", send(api.handleUnsend))
	mux.Handle("POST /messages/typing", send(api.handleTyping))
	mux.Handle("POST /messages/read", send(api.handleRead))
	mux.Handle("POST /requests/accept", send(api.handleAcceptRequest))

	mux.HandleFunc("/", func(writer http.ResponseWriter, _ *http.Request) {
		writeError(writer, newAPIError(http.StatusNotFound, codeNotFound, "Unknown endpoint."))
	})
	return api.logRequests(mux)
}

func (api *HTTPAPI) Serve(ctx context.Context) error {
	server := &http.Server{
		Addr:              api.config.ListenAddress(),
		Handler:           api.routes(),
		ReadHeaderTimeout: httpReadTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.ListenAndServe() }()
	api.log.Info().Str("event", "http_listening").Str("address", server.Addr).Send()
	select {
	case err := <-serveErrors:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}

func (api *HTTPAPI) requireSecret(header, expected string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !secretsEqual(request.Header.Get(header), expected) {
			writeError(writer, newAPIError(http.StatusUnauthorized, codeUnauthorized, "Missing or invalid secret."))
			return
		}
		next.ServeHTTP(writer, request)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (recorder *statusRecorder) WriteHeader(status int) {
	recorder.status = status
	recorder.ResponseWriter.WriteHeader(status)
}

func (api *HTTPAPI) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startedAt := time.Now()
		recorder := &statusRecorder{ResponseWriter: writer, status: http.StatusOK}
		next.ServeHTTP(recorder, request)
		path := request.URL.Path
		if strings.HasPrefix(path, "/media/") {
			path = "/media/{token}"
		}
		level := zerolog.DebugLevel
		if recorder.status >= 500 {
			level = zerolog.WarnLevel
		}
		api.log.WithLevel(level).Str("event", "http_request").Str("method", request.Method).Str("path", path).
			Int("status", recorder.status).Int64("duration_ms", time.Since(startedAt).Milliseconds()).Send()
	})
}

func readJSON(request *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(request.Body, maxRequestBodyBytes))
	if err := decoder.Decode(target); err != nil {
		return errBadRequest
	}
	return nil
}

func writeJSON(writer http.ResponseWriter, status int, body any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(body)
}

func writeOK(writer http.ResponseWriter, fields map[string]any) {
	body := map[string]any{"ok": true}
	for name, value := range fields {
		body[name] = value
	}
	writeJSON(writer, http.StatusOK, body)
}

func writeError(writer http.ResponseWriter, err error) {
	typed := asAPIError(err)
	writeJSON(writer, typed.status, map[string]any{"ok": false, "code": typed.code, "message": typed.message})
}

func (api *HTTPAPI) handleRoot(writer http.ResponseWriter, _ *http.Request) {
	writeOK(writer, map[string]any{"version": api.version})
}

type loginStartRequest struct {
	LoginToken   string `json:"login_token"`
	ConnectionID int64  `json:"connection_id"`
	Platform     string `json:"platform"`
}

func (api *HTTPAPI) handleLoginStart(writer http.ResponseWriter, request *http.Request) {
	var body loginStartRequest
	if err := readJSON(request, &body); err != nil || body.LoginToken == "" || body.ConnectionID <= 0 {
		writeError(writer, errBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), loginStepDeadline)
	defer cancel()
	step, err := api.logins.Start(ctx, body.LoginToken, body.ConnectionID, body.Platform)
	if err != nil {
		writeError(writer, err)
		return
	}
	writeOK(writer, map[string]any{"step": step})
}

type loginSubmitRequest struct {
	LoginToken string            `json:"login_token"`
	Input      map[string]string `json:"input"`
}

func (api *HTTPAPI) handleLoginSubmit(writer http.ResponseWriter, request *http.Request) {
	var body loginSubmitRequest
	if err := readJSON(request, &body); err != nil || body.LoginToken == "" {
		writeError(writer, errBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), loginStepDeadline)
	defer cancel()
	result, err := api.logins.Submit(ctx, body.LoginToken, body.Input)
	if err != nil {
		writeError(writer, err)
		return
	}
	if result.account != nil {
		writeOK(writer, map[string]any{"done": true, "account": result.account})
		return
	}
	writeOK(writer, map[string]any{"step": result.step, "waiting": result.waiting})
}

func (api *HTTPAPI) handleLoginCancel(writer http.ResponseWriter, request *http.Request) {
	var body struct {
		LoginToken string `json:"login_token"`
	}
	if err := readJSON(request, &body); err != nil || body.LoginToken == "" {
		writeError(writer, errBadRequest)
		return
	}
	api.logins.Cancel(body.LoginToken)
	writeOK(writer, nil)
}

type connectionRequest struct {
	ConnectionID int64 `json:"connection_id"`
	Chat         Chat  `json:"chat"`
}

func (api *HTTPAPI) handleLogout(writer http.ResponseWriter, request *http.Request) {
	var body connectionRequest
	if err := readJSON(request, &body); err != nil || body.ConnectionID <= 0 {
		writeError(writer, errBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), actionDeadline)
	defer cancel()
	if err := api.slots.Logout(ctx, body.ConnectionID); err != nil {
		writeError(writer, err)
		return
	}
	writeOK(writer, nil)
}

func (api *HTTPAPI) handleHealth(writer http.ResponseWriter, request *http.Request) {
	connectionID, err := strconv.ParseInt(request.URL.Query().Get("connection_id"), 10, 64)
	if err != nil || connectionID <= 0 {
		writeError(writer, errBadRequest)
		return
	}
	health := api.slots.Health(connectionID)
	health["bridge"] = map[string]any{
		"active_sessions": api.slots.ActiveSessionCount(),
		"max_sessions":    api.config.MaxSessions,
		"rss_bytes":       residentMemoryBytes(),
	}
	writeOK(writer, health)
}

func (api *HTTPAPI) handleMedia(writer http.ResponseWriter, request *http.Request) {
	file, err := api.media.Open(request.PathValue("token"))
	if err != nil {
		writeError(writer, err)
		return
	}
	defer file.handle.Close()
	writer.Header().Set("Content-Type", file.contentType)
	writer.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": file.filename}))
	writer.Header().Set("Cache-Control", "private, no-store")
	if file.size > 0 {
		writer.Header().Set("Content-Length", strconv.FormatInt(file.size, 10))
	}
	_, _ = io.Copy(writer, file.handle)
}

func (api *HTTPAPI) slotForAction(writer http.ResponseWriter, body *connectionRequest) (*Slot, bool) {
	if body.ConnectionID <= 0 || body.Chat.validate() != nil {
		writeError(writer, errBadRequest)
		return nil, false
	}
	slot, err := api.slots.ConnectedSlot(body.ConnectionID)
	if err != nil {
		writeError(writer, err)
		return nil, false
	}
	return slot, true
}

func (api *HTTPAPI) handleChatMembers(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	connectionID, _ := strconv.ParseInt(query.Get("connection_id"), 10, 64)
	chat := Chat{Kind: query.Get("chat_kind"), ID: query.Get("chat_id")}
	slot, ok := api.slotForAction(writer, &connectionRequest{ConnectionID: connectionID, Chat: chat})
	if !ok {
		return
	}
	members := slot.ChatMembers(request.Context(), chat)
	payload := make([]map[string]any, 0, len(members))
	for _, member := range members {
		payload = append(payload, member.payload())
	}
	writeOK(writer, map[string]any{"members": payload})
}

func (api *HTTPAPI) handleSend(writer http.ResponseWriter, request *http.Request) {
	var body SendRequest
	if err := readJSON(request, &body); err != nil {
		writeError(writer, errBadRequest)
		return
	}
	slot, ok := api.slotForAction(writer, &connectionRequest{ConnectionID: body.ConnectionID, Chat: body.Chat})
	if !ok {
		return
	}
	if body.Text == nil && body.Media == nil {
		writeError(writer, errBadRequest)
		return
	}
	deadline := sendTextDeadline
	if body.Media != nil {
		deadline = sendMediaDeadline
	}
	ctx, cancel := context.WithTimeout(request.Context(), deadline)
	defer cancel()
	result, err := slot.Send(ctx, &body)
	if err != nil {
		slot.log.Warn().Str("event", "send_failed").Int64("message_id", body.MessageID).
			Str("chat_kind", body.Chat.Kind).Str("chat_id", body.Chat.ID).Str("code", asAPIError(err).code).Err(err).Send()
		writeError(writer, err)
		return
	}
	writeOK(writer, map[string]any{"source_id": result.sourceID, "timings_ms": result.timings})
}

func (api *HTTPAPI) handleReaction(writer http.ResponseWriter, request *http.Request) {
	var body struct {
		connectionRequest
		TargetSourceID string `json:"target_source_id"`
		Emoji          string `json:"emoji"`
	}
	api.runAction(writer, request, &body, &body.connectionRequest, actionDeadline, "reaction", func(ctx context.Context, slot *Slot) error {
		return slot.React(ctx, body.Chat, body.TargetSourceID, body.Emoji)
	})
}

func (api *HTTPAPI) handleUnsend(writer http.ResponseWriter, request *http.Request) {
	var body struct {
		connectionRequest
		TargetSourceID string `json:"target_source_id"`
	}
	api.runAction(writer, request, &body, &body.connectionRequest, actionDeadline, "unsend", func(ctx context.Context, slot *Slot) error {
		return slot.Unsend(ctx, body.Chat, body.TargetSourceID)
	})
}

func (api *HTTPAPI) handleTyping(writer http.ResponseWriter, request *http.Request) {
	var body struct {
		connectionRequest
		Typing bool `json:"typing"`
	}
	api.runAction(writer, request, &body, &body.connectionRequest, typingDeadline, "typing", func(ctx context.Context, slot *Slot) error {
		return slot.SetTyping(ctx, body.Chat, body.Typing)
	})
}

func (api *HTTPAPI) handleRead(writer http.ResponseWriter, request *http.Request) {
	var body connectionRequest
	api.runAction(writer, request, &body, &body, actionDeadline, "read", func(ctx context.Context, slot *Slot) error {
		return slot.MarkRead(ctx, body.Chat)
	})
}

func (api *HTTPAPI) handleAcceptRequest(writer http.ResponseWriter, request *http.Request) {
	var body connectionRequest
	api.runAction(writer, request, &body, &body, actionDeadline, "accept_request", func(ctx context.Context, slot *Slot) error {
		return slot.AcceptRequest(ctx, body.Chat)
	})
}

func (api *HTTPAPI) runAction(writer http.ResponseWriter, request *http.Request, body any, connection *connectionRequest,
	deadline time.Duration, action string, run func(context.Context, *Slot) error) {
	if err := readJSON(request, body); err != nil {
		writeError(writer, errBadRequest)
		return
	}
	slot, ok := api.slotForAction(writer, connection)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), deadline)
	defer cancel()
	if err := run(ctx, slot); err != nil {
		slot.log.Warn().Str("event", action+"_failed").Str("chat_kind", connection.Chat.Kind).
			Str("chat_id", connection.Chat.ID).Str("code", asAPIError(err).code).Err(err).Send()
		writeError(writer, err)
		return
	}
	writeOK(writer, nil)
}

func residentMemoryBytes() int64 {
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(status), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		var kilobytes int64
		if _, err := fmt.Sscanf(strings.TrimSpace(strings.TrimPrefix(line, "VmRSS:")), "%d kB", &kilobytes); err == nil {
			return kilobytes * 1024
		}
	}
	return 0
}

var errStopped = errors.New("bridge is shutting down")
