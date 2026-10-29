package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "github.com/mutecomm/go-sqlcipher/v4"
	"go.mau.fi/whatsmeow/store/sqlstore"

	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

const (
	secretFileMode       = 0o600
	secretDirMode        = 0o700
	sessionFileExtension = ".session"
	sealedFormatVersion  = byte(1)
	sqlCipherDriverName  = "sqlite3"
	sqlCipherPageSize    = 4096
)

// storedSession is everything needed to resume one Messenger account without
// the password. It only ever touches disk sealed with BRIDGE_SESSION_KEY.
type storedSession struct {
	ConnectionID   int64                `json:"connection_id"`
	FacebookUserID int64                `json:"facebook_user_id"`
	Platform       types.Platform       `json:"platform"`
	Cookies        map[string]string    `json:"cookies"`
	NativeSession  *types.NativeSession `json:"native_session,omitempty"`
	E2EEDeviceID   uint16               `json:"e2ee_device_id,omitempty"`
	// LastRegularMessageMs bounds offline catch-up from the initial inbox
	// snapshot so a restart does not replay the whole inbox to Laravel.
	LastRegularMessageMs int64 `json:"last_regular_message_ms,omitempty"`
}

func newStoredSession(connectionID int64, platform types.Platform, sessionCookies *cookies.Cookies, native *types.NativeSession) *storedSession {
	session := &storedSession{ConnectionID: connectionID, Platform: platform, NativeSession: native}
	session.replaceCookies(sessionCookies)
	session.FacebookUserID = sessionCookies.GetUserID()
	return session
}

func (session *storedSession) replaceCookies(sessionCookies *cookies.Cookies) {
	values := make(map[string]string)
	for name, value := range sessionCookies.GetAll() {
		values[string(name)] = value
	}
	session.Cookies = values
}

func (session *storedSession) toCookies() *cookies.Cookies {
	sessionCookies := &cookies.Cookies{Platform: session.Platform}
	values := make(map[cookies.MetaCookieName]string, len(session.Cookies))
	for name, value := range session.Cookies {
		values[cookies.MetaCookieName(name)] = value
	}
	sessionCookies.UpdateValues(values)
	return sessionCookies
}

// secretSealer is AES-256-GCM with a random nonce per file. The associated
// data binds a file to its purpose so a sealed blob cannot be swapped between
// slots or reused as an outbox entry.
type secretSealer struct {
	aead cipher.AEAD
}

func newSecretSealer(key []byte) (*secretSealer, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &secretSealer{aead: aead}, nil
}

func (sealer *secretSealer) seal(plaintext []byte, purpose string) ([]byte, error) {
	nonce := make([]byte, sealer.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	sealed := append([]byte{sealedFormatVersion}, nonce...)
	return sealer.aead.Seal(sealed, nonce, plaintext, []byte(purpose)), nil
}

func (sealer *secretSealer) open(sealed []byte, purpose string) ([]byte, error) {
	nonceSize := sealer.aead.NonceSize()
	if len(sealed) < 1+nonceSize || sealed[0] != sealedFormatVersion {
		return nil, errors.New("sealed file has an unknown format")
	}
	plaintext, err := sealer.aead.Open(nil, sealed[1:1+nonceSize], sealed[1+nonceSize:], []byte(purpose))
	if err != nil {
		return nil, errors.New("sealed file cannot be decrypted with the configured key")
	}
	return plaintext, nil
}

func writeFileAtomically(path string, contents []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), secretDirMode); err != nil {
		return err
	}
	temporaryPath := path + ".tmp"
	if err := os.WriteFile(temporaryPath, contents, secretFileMode); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

type SessionStore struct {
	directory string
	sealer    *secretSealer
}

func NewSessionStore(directory string, key []byte) (*SessionStore, error) {
	sealer, err := newSecretSealer(key)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(directory, secretDirMode); err != nil {
		return nil, err
	}
	return &SessionStore{directory: directory, sealer: sealer}, nil
}

func sessionPurpose(connectionID int64) string {
	return "session:" + strconv.FormatInt(connectionID, 10)
}

func (store *SessionStore) path(connectionID int64) string {
	return filepath.Join(store.directory, strconv.FormatInt(connectionID, 10)+sessionFileExtension)
}

func (store *SessionStore) Save(session *storedSession) error {
	plaintext, err := json.Marshal(session)
	if err != nil {
		return err
	}
	sealed, err := store.sealer.seal(plaintext, sessionPurpose(session.ConnectionID))
	if err != nil {
		return err
	}
	return writeFileAtomically(store.path(session.ConnectionID), sealed)
}

func (store *SessionStore) Load(connectionID int64) (*storedSession, error) {
	sealed, err := os.ReadFile(store.path(connectionID))
	if err != nil {
		return nil, err
	}
	plaintext, err := store.sealer.open(sealed, sessionPurpose(connectionID))
	if err != nil {
		return nil, err
	}
	var session storedSession
	if err := json.Unmarshal(plaintext, &session); err != nil {
		return nil, fmt.Errorf("session file is unreadable: %w", err)
	}
	return &session, nil
}

func (store *SessionStore) Delete(connectionID int64) error {
	err := os.Remove(store.path(connectionID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (store *SessionStore) ConnectionIDs() ([]int64, error) {
	entries, err := os.ReadDir(store.directory)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, sessionFileExtension) {
			continue
		}
		if id, err := strconv.ParseInt(strings.TrimSuffix(name, sessionFileExtension), 10, 64); err == nil {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// sqlCipherDSN builds the data source for the encrypted device database. The
// result contains the raw key, so it must never be logged or put in an error.
func sqlCipherDSN(path string, key []byte) string {
	return fmt.Sprintf("file:%s?_pragma_key=x'%s'&_pragma_cipher_page_size=%d&_foreign_keys=on&_busy_timeout=5000",
		path, strings.ToUpper(hex.EncodeToString(key)), sqlCipherPageSize)
}

// OpenDeviceStore opens the end-to-end encryption key store for every slot:
// one SQLCipher database, the whole file encrypted with BRIDGE_DEVICE_KEY.
func OpenDeviceStore(ctx context.Context, config *Config, loggers *Loggers) (*sqlstore.Container, error) {
	if err := os.MkdirAll(filepath.Dir(config.DeviceDBPath()), secretDirMode); err != nil {
		return nil, err
	}
	container, err := sqlstore.New(ctx, sqlCipherDriverName, sqlCipherDSN(config.DeviceDBPath(), config.DeviceKey), loggers.WhatsApp("device_store"))
	if err != nil {
		if strings.Contains(err.Error(), "file is not a database") {
			return nil, errors.New("device store cannot be opened: wrong BRIDGE_DEVICE_KEY or the file is not encrypted")
		}
		return nil, fmt.Errorf("device store cannot be opened: %s", redactKey(err.Error(), config.DeviceKey))
	}
	if err := os.Chmod(config.DeviceDBPath(), secretFileMode); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return container, nil
}

func redactKey(message string, key []byte) string {
	encoded := hex.EncodeToString(key)
	message = strings.ReplaceAll(message, encoded, redactedMarker)
	return strings.ReplaceAll(message, strings.ToUpper(encoded), redactedMarker)
}
