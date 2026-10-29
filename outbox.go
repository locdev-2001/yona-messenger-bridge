package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

const (
	outboxEntryExtension     = ".evt"
	outboxPurpose            = "outbox"
	webhookSecretHeader      = "X-Messenger-Bridge-Secret"
	webhookRequestTimeout    = 15 * time.Second
	unauthorizedRetryDelay   = 30 * time.Second
	firstTransientRetryDelay = 30 * time.Second
	maxTransientRetryDelay   = time.Hour
	idleOutboxPoll           = 10 * time.Second
	responseDrainLimit       = 64 << 10
)

// WebhookEvent is one sidecar → Laravel event. Every event carries type,
// connection_id, facebook_user_id and occurred_at; the rest depends on type.
type WebhookEvent map[string]any

func newWebhookEvent(eventType string, connectionID, facebookUserID int64, occurredAt time.Time) WebhookEvent {
	if occurredAt.IsZero() {
		occurredAt = time.Now()
	}
	return WebhookEvent{
		"type":             eventType,
		"connection_id":    connectionID,
		"facebook_user_id": nullableString(formatFBID(facebookUserID)),
		"occurred_at":      occurredAt.UnixMilli(),
	}
}

func (event WebhookEvent) eventType() string {
	eventType, _ := event["type"].(string)
	return eventType
}

type deliveryOutcome int

const (
	deliveryDone deliveryOutcome = iota
	deliveryUnauthorized
	deliveryRejected
	deliveryTransient
)

type WebhookClient struct {
	url        string
	secret     string
	httpClient *http.Client
}

func (client *WebhookClient) deliver(ctx context.Context, body []byte) (deliveryOutcome, int, error) {
	if client.url == "" {
		return deliveryTransient, 0, errors.New("LARAVEL_BASE_URL is not configured")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.url, bytes.NewReader(body))
	if err != nil {
		return deliveryRejected, 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set(webhookSecretHeader, client.secret)
	response, err := client.httpClient.Do(request)
	if err != nil {
		return deliveryTransient, 0, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, responseDrainLimit))
	_ = response.Body.Close()
	status := response.StatusCode
	switch {
	case status >= 200 && status < 300:
		return deliveryDone, status, nil
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return deliveryUnauthorized, status, nil
	case status == http.StatusTooManyRequests || status >= 500:
		return deliveryTransient, status, nil
	default:
		return deliveryRejected, status, nil
	}
}

// Outbox is a durable FIFO of webhook events. One worker delivers entries in
// order so Laravel sees messages in the sequence Meta delivered them; entries
// survive Laravel restarts and bridge restarts. Single process only.
type Outbox struct {
	directory     string
	deadDirectory string
	sealer        *secretSealer
	webhook       *WebhookClient
	log           zerolog.Logger
	wake          chan struct{}
	sequence      atomic.Uint64
	enqueueLock   sync.Mutex
}

func NewOutbox(config *Config, sealer *secretSealer, log zerolog.Logger) (*Outbox, error) {
	for _, directory := range []string{config.OutboxDir(), config.DeadLetterDir()} {
		if err := os.MkdirAll(directory, secretDirMode); err != nil {
			return nil, err
		}
	}
	return &Outbox{
		directory:     config.OutboxDir(),
		deadDirectory: config.DeadLetterDir(),
		sealer:        sealer,
		webhook: &WebhookClient{
			url:        config.WebhookURL,
			secret:     config.WebhookSecret,
			httpClient: &http.Client{Timeout: webhookRequestTimeout},
		},
		log:  log,
		wake: make(chan struct{}, 1),
	}, nil
}

// Enqueue persists the event before returning, so a caller may acknowledge the
// upstream message to Meta once Enqueue succeeds.
func (outbox *Outbox) Enqueue(event WebhookEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	sealed, err := outbox.sealer.seal(body, outboxPurpose)
	if err != nil {
		return err
	}
	outbox.enqueueLock.Lock()
	name := fmt.Sprintf("%020d-%08d%s", time.Now().UnixNano(), outbox.sequence.Add(1), outboxEntryExtension)
	err = writeFileAtomically(filepath.Join(outbox.directory, name), sealed)
	outbox.enqueueLock.Unlock()
	if err != nil {
		return err
	}
	select {
	case outbox.wake <- struct{}{}:
	default:
	}
	return nil
}

// SendNow delivers an ephemeral event once (typing, receipts, reactions).
func (outbox *Outbox) SendNow(event WebhookEvent) {
	body, err := json.Marshal(event)
	if err != nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), webhookRequestTimeout)
		defer cancel()
		outcome, status, err := outbox.webhook.deliver(ctx, body)
		if outcome != deliveryDone {
			outbox.log.Warn().Str("event", "webhook_ephemeral_failed").Str("webhook_type", event.eventType()).
				Int("status", status).AnErr("error", err).Send()
		}
	}()
}

func (outbox *Outbox) Run(ctx context.Context) {
	for ctx.Err() == nil {
		name, found := outbox.oldestEntry()
		if !found {
			select {
			case <-ctx.Done():
				return
			case <-outbox.wake:
			case <-time.After(idleOutboxPoll):
			}
			continue
		}
		outbox.deliverEntry(ctx, name)
	}
}

func (outbox *Outbox) oldestEntry() (string, bool) {
	entries, err := os.ReadDir(outbox.directory)
	if err != nil {
		outbox.log.Error().Str("event", "outbox_read_failed").Err(err).Send()
		return "", false
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), outboxEntryExtension) {
			names = append(names, entry.Name())
		}
	}
	if len(names) == 0 {
		return "", false
	}
	sort.Strings(names)
	return names[0], true
}

func (outbox *Outbox) deliverEntry(ctx context.Context, name string) {
	path := filepath.Join(outbox.directory, name)
	sealed, err := os.ReadFile(path)
	if err != nil {
		outbox.moveToDeadLetters(name, "unreadable")
		return
	}
	body, err := outbox.sealer.open(sealed, outboxPurpose)
	if err != nil {
		outbox.moveToDeadLetters(name, "undecryptable")
		return
	}
	var envelope struct {
		Type         string `json:"type"`
		ConnectionID int64  `json:"connection_id"`
		SourceID     string `json:"source_id"`
	}
	_ = json.Unmarshal(body, &envelope)
	logEvent := func(level zerolog.Level, event string) *zerolog.Event {
		return outbox.log.WithLevel(level).Str("event", event).Str("webhook_type", envelope.Type).
			Int64("connection_id", envelope.ConnectionID).Str("source_id", envelope.SourceID)
	}
	transientAttempts := 0
	for ctx.Err() == nil {
		startedAt := time.Now()
		attemptCtx, cancel := context.WithTimeout(ctx, webhookRequestTimeout)
		outcome, status, err := outbox.webhook.deliver(attemptCtx, body)
		cancel()
		switch outcome {
		case deliveryDone:
			_ = os.Remove(path)
			logEvent(zerolog.DebugLevel, "webhook_delivered").Int("status", status).
				Int64("duration_ms", time.Since(startedAt).Milliseconds()).Send()
			return
		case deliveryUnauthorized:
			logEvent(zerolog.ErrorLevel, "webhook_unauthorized").Int("status", status).Send()
			if !sleepContext(ctx, unauthorizedRetryDelay) {
				return
			}
		case deliveryRejected:
			logEvent(zerolog.ErrorLevel, "webhook_rejected").Int("status", status).AnErr("error", err).Send()
			outbox.moveToDeadLetters(name, "rejected")
			return
		case deliveryTransient:
			transientAttempts++
			// Never dead-lettered: when Laravel is down every event fails, and a
			// blocked FIFO is better than a lost inbound message.
			delay := min(firstTransientRetryDelay<<min(transientAttempts-1, 7), maxTransientRetryDelay)
			logEvent(zerolog.WarnLevel, "webhook_retry").Int("status", status).Int("attempt", transientAttempts).
				Int64("retry_in_ms", delay.Milliseconds()).AnErr("error", err).Send()
			if !sleepContext(ctx, delay) {
				return
			}
		}
	}
}

func (outbox *Outbox) moveToDeadLetters(name, reason string) {
	if err := os.Rename(filepath.Join(outbox.directory, name), filepath.Join(outbox.deadDirectory, name)); err != nil {
		outbox.log.Error().Str("event", "outbox_dead_letter_failed").Err(err).Send()
		_ = os.Remove(filepath.Join(outbox.directory, name))
		return
	}
	outbox.log.Warn().Str("event", "outbox_dead_letter").Str("reason", reason).Str("entry", name).Send()
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func secretsEqual(provided, expected string) bool {
	if expected == "" || provided == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) == 1
}
