package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	globalLog "github.com/rs/zerolog/log"
	waLog "go.mau.fi/whatsmeow/util/log"
)

const (
	logFileMode        = 0o640
	redactedMarker     = "[redacted]"
	maxLoggedFieldSize = 512
)

// Fields that libraries fill with raw protocol payloads: message bodies, poll
// questions, login form values, decoded tables. They never reach the log.
var redactedLogFields = map[string]bool{
	"data": true, "val": true, "step_data": true, "response": true, "resp": true,
	"table": true, "payload": true, "body": true, "text": true, "response_data": true,
	"block_response": true, "info": true, "transport": true, "application": true,
	"ig_transport": true, "create_resp": true, "task": true, "inputs": true,
	"cookies": true, "cookie_names": true, "password": true, "username": true,
	"access_token": true, "token": true, "message_text": true, "reply_text": true,
}

var xmlNodePattern = regexp.MustCompile(`<[a-zA-Z/!?][^>]*>`)

// redactingWriter receives one JSON log line per Write and strips any field or
// fragment that may carry user content before the line reaches a sink.
type redactingWriter struct {
	mu   sync.Mutex
	sink io.Writer
}

func (writer *redactingWriter) Write(line []byte) (int, error) {
	cleaned := redactLogLine(line)
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if _, err := writer.sink.Write(cleaned); err != nil {
		return 0, err
	}
	return len(line), nil
}

func redactLogLine(line []byte) []byte {
	var fields map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(line), &fields); err != nil {
		return []byte(fmt.Sprintf(`{"level":"warn","event":"log_line_dropped","time":%q}`+"\n", time.Now().Format(time.RFC3339)))
	}
	for name, value := range fields {
		if redactedLogFields[strings.ToLower(name)] {
			delete(fields, name)
			continue
		}
		if text, isText := value.(string); isText {
			fields[name] = redactText(text)
		} else if _, isNested := value.(map[string]any); isNested {
			delete(fields, name)
		} else if _, isList := value.([]any); isList {
			delete(fields, name)
		}
	}
	cleaned, err := json.Marshal(fields)
	if err != nil {
		return line
	}
	return append(cleaned, '\n')
}

func redactText(text string) string {
	if xmlNodePattern.MatchString(text) {
		text = xmlNodePattern.ReplaceAllString(text, redactedMarker)
	}
	if len(text) > maxLoggedFieldSize {
		text = text[:maxLoggedFieldSize] + "…"
	}
	return text
}

type Loggers struct {
	App     zerolog.Logger
	Library zerolog.Logger
	closer  io.Closer
}

func (loggers *Loggers) Close() {
	if loggers.closer != nil {
		_ = loggers.closer.Close()
	}
}

// WhatsApp returns the encrypted-transport logger, routed through the same
// redacting sink as every other library log line.
func (loggers *Loggers) WhatsApp(module string) waLog.Logger {
	return waLog.Zerolog(loggers.Library.With().Str("component", module).Logger())
}

func NewLoggers(config *Config) (*Loggers, error) {
	sinks := []io.Writer{os.Stdout}
	loggers := &Loggers{}
	if config.LogFile != "" {
		file, err := os.OpenFile(config.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, logFileMode)
		if err != nil {
			return nil, fmt.Errorf("open BRIDGE_LOG_FILE: %w", err)
		}
		sinks = append(sinks, file)
		loggers.closer = file
	}
	writer := &redactingWriter{sink: io.MultiWriter(sinks...)}
	zerolog.TimeFieldFormat = time.RFC3339Nano
	base := zerolog.New(writer).With().Timestamp().Logger()
	loggers.App = base.Level(config.LogLevel).With().Str("component", "bridge").Logger()
	loggers.Library = base.Level(config.LibraryLogLevel)
	// table.WrapMessages and a few other library paths log through the global
	// zerolog logger directly, so it must go through the redacting sink too.
	globalLog.Logger = loggers.Library.With().Str("component", "global").Logger()
	zerolog.DefaultContextLogger = &globalLog.Logger
	return loggers, nil
}
