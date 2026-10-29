// Command messenger-bridge keeps personal Facebook Messenger sessions alive
// for Yona inboxes. One process serves every connected account: it listens on
// the regular and the end-to-end encrypted Messenger transports, sends on
// behalf of Laravel and reports events back through a durable webhook outbox.
//
// Usage:
//
//	messenger-bridge                       run the bridge
//	messenger-bridge import-session --connection-id N --from DIR
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
)

const developmentVersion = "dev"

// version is stamped at build time: go build -ldflags "-X main.version=<sha>".
var version = developmentVersion

func main() {
	if len(os.Args) > 1 && os.Args[1] == "import-session" {
		if err := runImportSession(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "import-session:", err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "messenger-bridge:", err)
		os.Exit(1)
	}
}

func run() error {
	config, err := LoadConfig()
	if err != nil {
		return err
	}
	loggers, err := NewLoggers(config)
	if err != nil {
		return err
	}
	defer loggers.Close()
	log := loggers.App

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sealer, err := newSecretSealer(config.SessionKey)
	if err != nil {
		return err
	}
	sessions, err := NewSessionStore(config.SessionsDir(), config.SessionKey)
	if err != nil {
		return fmt.Errorf("session store: %w", err)
	}
	devices, err := OpenDeviceStore(ctx, config, loggers)
	if err != nil {
		return err
	}
	outbox, err := NewOutbox(config, sealer, log.With().Str("component", "outbox").Logger())
	if err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	media, err := NewMediaStore(config.MediaDir(), log.With().Str("component", "media").Logger())
	if err != nil {
		return fmt.Errorf("media store: %w", err)
	}
	slots := NewSlotManager(ctx, config, loggers, sessions, devices, outbox, media)
	logins := NewLoginManager(slots, loggers)
	api := &HTTPAPI{config: config, slots: slots, logins: logins, media: media, log: log, version: buildVersion()}

	log.Info().Str("event", "bridge_starting").Str("version", api.version).Str("environment", config.Environment).
		Int("max_sessions", config.MaxSessions).Send()
	go outbox.Run(ctx)
	go media.RunJanitor(ctx)
	go logins.RunJanitor(ctx)
	go slots.RunSessionSaver(ctx)
	go slots.RestoreAll()

	serveErr := api.Serve(ctx)
	stop()
	slots.Shutdown()
	_ = devices.Close()
	log.Info().Str("event", "bridge_stopped").Send()
	return serveErr
}

func buildVersion() string {
	if version != developmentVersion {
		return version
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return developmentVersion
	}
	revision, modified := "", false
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	if revision == "" {
		return developmentVersion
	}
	if len(revision) > 12 {
		revision = revision[:12]
	}
	if modified {
		revision += "-dirty"
	}
	return revision
}
