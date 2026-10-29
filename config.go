package main

import (
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
)

const (
	productionEnvironment = "production"

	defaultHTTPHost         = "127.0.0.1"
	defaultHTTPPort         = 8614
	defaultStateDir         = "./state"
	defaultMaxSessions      = 50
	defaultMaxMediaBytes    = 25 << 20
	defaultLibraryLogLevel  = "warn"
	defaultLogLevel         = "info"
	requiredSecretKeyLength = 32
	webhookPath             = "/webhooks/messenger-personal"
	dotEnvFileName          = ".env"
	listFieldSeparator      = ","
)

type Config struct {
	Environment        string
	HTTPHost           string
	HTTPPort           int
	PublicURL          string
	UISecret           string
	SendSecret         string
	WebhookSecret      string
	LaravelBaseURL     string
	WebhookURL         string
	SessionKey         []byte
	DeviceKey          []byte
	StateDir           string
	MaxSessions        int
	MaxMediaBytes      int64
	FetchHostAllowlist map[string]bool
	LogFile            string
	LogLevel           zerolog.Level
	LibraryLogLevel    zerolog.Level
}

func (config *Config) IsProduction() bool {
	return config.Environment == productionEnvironment
}

func (config *Config) ListenAddress() string {
	return net.JoinHostPort(config.HTTPHost, strconv.Itoa(config.HTTPPort))
}

func (config *Config) SessionsDir() string { return filepath.Join(config.StateDir, "sessions") }
func (config *Config) OutboxDir() string   { return filepath.Join(config.StateDir, "outbox") }
func (config *Config) DeadLetterDir() string {
	return filepath.Join(config.StateDir, "outbox-dead")
}
func (config *Config) MediaDir() string     { return filepath.Join(config.StateDir, "media-tmp") }
func (config *Config) DeviceDBPath() string { return filepath.Join(config.StateDir, "devices.db") }

// LoadConfig reads the process environment, after filling unset variables
// from a .env file in the working directory, and refuses to boot when a
// production deployment is missing a secret or would leak traffic.
func LoadConfig() (*Config, error) {
	if err := loadDotEnv(dotEnvFileName); err != nil {
		return nil, err
	}
	config := &Config{
		Environment:    strings.TrimSpace(os.Getenv("BRIDGE_ENV")),
		HTTPHost:       envOrDefault("BRIDGE_HTTP_HOST", defaultHTTPHost),
		UISecret:       os.Getenv("BRIDGE_UI_SECRET"),
		SendSecret:     os.Getenv("BRIDGE_SEND_SECRET"),
		WebhookSecret:  os.Getenv("LARAVEL_WEBHOOK_SECRET"),
		LaravelBaseURL: strings.TrimRight(strings.TrimSpace(os.Getenv("LARAVEL_BASE_URL")), "/"),
		StateDir:       envOrDefault("BRIDGE_STATE_DIR", defaultStateDir),
		LogFile:        strings.TrimSpace(os.Getenv("BRIDGE_LOG_FILE")),
	}
	var err error
	if config.HTTPPort, err = envInt("BRIDGE_HTTP_PORT", defaultHTTPPort); err != nil {
		return nil, err
	}
	if config.MaxSessions, err = envInt("BRIDGE_MAX_SESSIONS", defaultMaxSessions); err != nil {
		return nil, err
	}
	maxMediaBytes, err := envInt("BRIDGE_MAX_MEDIA_BYTES", defaultMaxMediaBytes)
	if err != nil {
		return nil, err
	}
	config.MaxMediaBytes = int64(maxMediaBytes)
	if config.LogLevel, err = zerolog.ParseLevel(envOrDefault("BRIDGE_LOG_LEVEL", defaultLogLevel)); err != nil {
		return nil, fmt.Errorf("BRIDGE_LOG_LEVEL: %w", err)
	}
	if config.LibraryLogLevel, err = zerolog.ParseLevel(envOrDefault("BRIDGE_LIBRARY_LOG_LEVEL", defaultLibraryLogLevel)); err != nil {
		return nil, fmt.Errorf("BRIDGE_LIBRARY_LOG_LEVEL: %w", err)
	}
	config.PublicURL = strings.TrimRight(envOrDefault("BRIDGE_PUBLIC_URL", "http://"+config.ListenAddress()), "/")
	if config.LaravelBaseURL != "" {
		config.WebhookURL = config.LaravelBaseURL + webhookPath
	}
	if config.SessionKey, err = decodeSecretKey("BRIDGE_SESSION_KEY"); err != nil {
		return nil, err
	}
	if config.DeviceKey, err = decodeSecretKey("BRIDGE_DEVICE_KEY"); err != nil {
		return nil, err
	}
	config.FetchHostAllowlist = buildFetchAllowlist(os.Getenv("BRIDGE_FETCH_HOST_ALLOWLIST"), config.LaravelBaseURL)
	if err := config.validate(); err != nil {
		return nil, err
	}
	return config, nil
}

func (config *Config) validate() error {
	if !isLoopbackHost(config.HTTPHost) {
		return fmt.Errorf("BRIDGE_HTTP_HOST must be a loopback address, got %q", config.HTTPHost)
	}
	if config.MaxSessions < 1 {
		return errors.New("BRIDGE_MAX_SESSIONS must be at least 1")
	}
	if config.SessionKey == nil {
		return errors.New("BRIDGE_SESSION_KEY is required (32 bytes, base64)")
	}
	if config.DeviceKey == nil {
		return errors.New("BRIDGE_DEVICE_KEY is required (32 bytes, base64)")
	}
	if !config.IsProduction() {
		return nil
	}
	missing := make([]string, 0, 4)
	for name, value := range map[string]string{
		"BRIDGE_UI_SECRET":       config.UISecret,
		"BRIDGE_SEND_SECRET":     config.SendSecret,
		"LARAVEL_WEBHOOK_SECRET": config.WebhookSecret,
		"LARAVEL_BASE_URL":       config.LaravelBaseURL,
	} {
		if strings.TrimSpace(value) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("production boot refused, missing: %s", strings.Join(missing, ", "))
	}
	if !strings.HasPrefix(config.LaravelBaseURL, "https://") {
		return errors.New("production boot refused: LARAVEL_BASE_URL must use https")
	}
	return nil
}

func decodeSecretKey(name string) ([]byte, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("%s is not valid base64", name)
	}
	if len(key) != requiredSecretKeyLength {
		return nil, fmt.Errorf("%s must decode to %d bytes, got %d", name, requiredSecretKeyLength, len(key))
	}
	return key, nil
}

func buildFetchAllowlist(rawList, laravelBaseURL string) map[string]bool {
	hosts := make(map[string]bool)
	for _, host := range strings.Split(rawList, listFieldSeparator) {
		if host = strings.ToLower(strings.TrimSpace(host)); host != "" {
			hosts[host] = true
		}
	}
	if parsed, err := url.Parse(laravelBaseURL); err == nil && parsed.Host != "" {
		hosts[strings.ToLower(parsed.Host)] = true
	}
	return hosts
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	return value, nil
}

// loadDotEnv sets variables from a KEY=VALUE file without overriding what the
// process environment (supervisor, shell) already provides.
func loadDotEnv(path string) error {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, found := strings.Cut(strings.TrimPrefix(line, "export "), "=")
		if !found {
			continue
		}
		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)
		if len(value) >= 2 && (value[0] == '"' || value[0] == '\'') && value[len(value)-1] == value[0] {
			value = value[1 : len(value)-1]
		}
		if _, alreadySet := os.LookupEnv(name); !alreadySet {
			_ = os.Setenv(name, value)
		}
	}
	return scanner.Err()
}
