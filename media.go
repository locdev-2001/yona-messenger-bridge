package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
)

const (
	mediaTokenTTL         = time.Hour
	mediaJanitorInterval  = 5 * time.Minute
	mediaTokenBytes       = 32
	mediaDecryptTimeout   = 2 * time.Minute
	outboundFetchTimeout  = 90 * time.Second
	maxOutboundRedirects  = 3
	defaultBinaryMimeType = "application/octet-stream"
)

// encryptedMedia is an end-to-end encrypted attachment the bridge can fetch
// and decrypt on demand. Decryption needs the account's live encrypted client,
// so the entry remembers which slot it belongs to.
type encryptedMedia struct {
	connectionID int64
	integral     *waMediaTransport.WAMediaTransport_Integral
	mediaType    whatsmeow.MediaType
	contentType  string
	filename     string
	size         int64
	expiresAt    time.Time

	once     sync.Once
	filePath string
	fetchErr error
}

type openedMedia struct {
	handle      *os.File
	contentType string
	filename    string
	size        int64
}

// MediaStore issues /media/{token} links for decrypted encrypted-transport
// attachments. Files are decrypted on first request, kept for an hour and
// served as many times as Laravel asks within that hour.
type MediaStore struct {
	directory string
	slots     func(connectionID int64) *Slot
	log       zerolog.Logger

	mu      sync.Mutex
	entries map[string]*encryptedMedia
}

func NewMediaStore(directory string, log zerolog.Logger) (*MediaStore, error) {
	if err := os.RemoveAll(directory); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(directory, secretDirMode); err != nil {
		return nil, err
	}
	return &MediaStore{directory: directory, log: log, entries: make(map[string]*encryptedMedia)}, nil
}

func (store *MediaStore) Register(media *encryptedMedia) (string, error) {
	tokenBytes := make([]byte, mediaTokenBytes)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes)
	media.expiresAt = time.Now().Add(mediaTokenTTL)
	store.mu.Lock()
	store.entries[token] = media
	store.mu.Unlock()
	return token, nil
}

func (store *MediaStore) Open(token string) (*openedMedia, error) {
	store.mu.Lock()
	media, exists := store.entries[token]
	store.mu.Unlock()
	if !exists || time.Now().After(media.expiresAt) {
		return nil, newAPIError(http.StatusNotFound, codeNotFound, "The media link expired or does not exist.")
	}
	media.once.Do(func() {
		// Detached from the request: a cancelled first download must not
		// poison the entry for the retries Laravel makes afterwards.
		decryptCtx, cancel := context.WithTimeout(context.Background(), mediaDecryptTimeout)
		defer cancel()
		media.filePath, media.fetchErr = store.decryptToFile(decryptCtx, media)
	})
	if media.fetchErr != nil {
		return nil, media.fetchErr
	}
	handle, err := os.Open(media.filePath)
	if err != nil {
		return nil, errMediaFetch
	}
	info, err := handle.Stat()
	if err != nil {
		_ = handle.Close()
		return nil, errMediaFetch
	}
	return &openedMedia{handle: handle, contentType: media.contentType, filename: media.filename, size: info.Size()}, nil
}

func (store *MediaStore) decryptToFile(ctx context.Context, media *encryptedMedia) (string, error) {
	slot := store.slots(media.connectionID)
	if slot == nil {
		return "", errSlotNotFound
	}
	client := slot.encryptedClient()
	if client == nil {
		return "", errSlotNotReady
	}
	startedAt := time.Now()
	plaintext, err := client.DownloadFB(ctx, media.integral, media.mediaType)
	if err != nil {
		slot.log.Warn().Str("event", "media_decrypt_failed").Err(err).Send()
		return "", errMediaFetch
	}
	file, err := os.CreateTemp(store.directory, "media-*")
	if err != nil {
		return "", errUnknownFailure
	}
	defer file.Close()
	if err := file.Chmod(secretFileMode); err != nil {
		return "", errUnknownFailure
	}
	if _, err := file.Write(plaintext); err != nil {
		_ = os.Remove(file.Name())
		return "", errUnknownFailure
	}
	slot.log.Debug().Str("event", "media_decrypted").Int("bytes", len(plaintext)).
		Int64("duration_ms", time.Since(startedAt).Milliseconds()).Send()
	return file.Name(), nil
}

func (store *MediaStore) RunJanitor(ctx context.Context) {
	ticker := time.NewTicker(mediaJanitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			store.removeExpired()
		}
	}
}

func (store *MediaStore) removeExpired() {
	now := time.Now()
	store.mu.Lock()
	expired := make([]*encryptedMedia, 0)
	for token, media := range store.entries {
		if now.After(media.expiresAt) {
			expired = append(expired, media)
			delete(store.entries, token)
		}
	}
	store.mu.Unlock()
	for _, media := range expired {
		if media.filePath != "" {
			_ = os.Remove(media.filePath)
		}
	}
}

// fetchedMedia is an attachment Laravel asked the bridge to send.
type fetchedMedia struct {
	data        []byte
	contentType string
	filename    string
}

type OutboundFetcher struct {
	allowedHosts map[string]bool
	maxBytes     int64
	httpClient   *http.Client
}

func NewOutboundFetcher(config *Config) *OutboundFetcher {
	fetcher := &OutboundFetcher{allowedHosts: config.FetchHostAllowlist, maxBytes: config.MaxMediaBytes}
	fetcher.httpClient = &http.Client{
		Timeout: outboundFetchTimeout,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= maxOutboundRedirects || !fetcher.isAllowed(request.URL) {
				return errors.New("redirect target is not allowed")
			}
			return nil
		},
	}
	return fetcher
}

// isAllowed matches an allowlist entry exactly as written: "host:port"
// only accepts that port, a bare "host" accepts any port. Plain http is only
// allowed towards the local machine (Laravel's APP_URL in development).
func (fetcher *OutboundFetcher) isAllowed(target *url.URL) bool {
	hostname := strings.ToLower(target.Hostname())
	switch target.Scheme {
	case "https":
	case "http":
		if !isLoopbackHost(hostname) {
			return false
		}
	default:
		return false
	}
	return fetcher.allowedHosts[strings.ToLower(target.Host)] || fetcher.allowedHosts[hostname]
}

func (fetcher *OutboundFetcher) Fetch(ctx context.Context, media *SendMedia) (*fetchedMedia, error) {
	target, err := url.Parse(media.URL)
	if err != nil || !fetcher.isAllowed(target) {
		return nil, newAPIError(http.StatusBadGateway, codeMediaFetchFailed, "The media host is not allowed.")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, errMediaFetch
	}
	response, err := fetcher.httpClient.Do(request)
	if err != nil {
		return nil, errMediaFetch
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errMediaFetch
	}
	if response.ContentLength > fetcher.maxBytes {
		return nil, newAPIError(http.StatusBadGateway, codeMediaFetchFailed, "The media is larger than the bridge allows.")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, fetcher.maxBytes+1))
	if err != nil {
		return nil, errMediaFetch
	}
	if int64(len(data)) > fetcher.maxBytes {
		return nil, newAPIError(http.StatusBadGateway, codeMediaFetchFailed, "The media is larger than the bridge allows.")
	}
	contentType := media.ContentType
	if contentType == "" {
		contentType, _, _ = mime.ParseMediaType(response.Header.Get("Content-Type"))
	}
	if contentType == "" || contentType == defaultBinaryMimeType {
		contentType = http.DetectContentType(data)
	}
	if parsed, _, err := mime.ParseMediaType(contentType); err == nil {
		contentType = parsed
	}
	filename := media.Filename
	if filename == "" {
		filename = filepath.Base(target.Path)
	}
	return &fetchedMedia{data: data, contentType: contentType, filename: filename}, nil
}

func defaultFilename(kind, contentType string) string {
	extension := ""
	if extensions, err := mime.ExtensionsByType(contentType); err == nil && len(extensions) > 0 {
		extension = extensions[0]
	}
	return fmt.Sprintf("%s%s", kind, extension)
}
