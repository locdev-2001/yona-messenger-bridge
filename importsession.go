package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

const (
	probeSessionFile    = "session.json"
	probeDeviceDBFile   = "device.db"
	attachedProbeSchema = "probe"
	deviceTableName     = "whatsmeow_device"
	versionTableName    = "whatsmeow_version"
)

// probeSession is the plain session file written by the feasibility probe.
type probeSession struct {
	Platform      types.Platform       `json:"platform"`
	Cookies       map[string]string    `json:"cookies"`
	NativeSession *types.NativeSession `json:"native_session,omitempty"`
	E2EEDeviceID  uint16               `json:"e2ee_device_id,omitempty"`
}

// runImportSession is a development aid: it moves a session created by the
// probe into this bridge's encrypted formats for one slot, so a test account
// does not have to log in again. The bridge must not be running.
func runImportSession(arguments []string) error {
	flags := flag.NewFlagSet("import-session", flag.ContinueOnError)
	connectionID := flags.Int64("connection-id", 0, "slot (channel_messenger_personal.id) to import into")
	sourceDir := flags.String("from", "", "probe state directory containing session.json and device.db")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *connectionID <= 0 || *sourceDir == "" {
		return errors.New("both --connection-id and --from are required")
	}
	config, err := LoadConfig()
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(*sourceDir, probeSessionFile))
	if err != nil {
		return fmt.Errorf("read probe session: %w", err)
	}
	var source probeSession
	if err := json.Unmarshal(raw, &source); err != nil {
		return errors.New("probe session file is not valid JSON")
	}
	session := &storedSession{
		ConnectionID:  *connectionID,
		Platform:      source.Platform,
		Cookies:       source.Cookies,
		NativeSession: source.NativeSession,
		E2EEDeviceID:  source.E2EEDeviceID,
	}
	session.FacebookUserID = session.toCookies().GetUserID()
	if session.FacebookUserID == 0 {
		return errors.New("probe session has no logged-in account")
	}

	if source.E2EEDeviceID != 0 {
		probeDB := filepath.Join(*sourceDir, probeDeviceDBFile)
		if _, err := os.Stat(probeDB); err != nil {
			return fmt.Errorf("probe device database: %w", err)
		}
		if err := importDeviceKeys(config, probeDB); err != nil {
			return err
		}
	}
	sessions, err := NewSessionStore(config.SessionsDir(), config.SessionKey)
	if err != nil {
		return err
	}
	if err := sessions.Save(session); err != nil {
		return fmt.Errorf("save session: %w", err)
	}
	fmt.Printf("imported account %d into slot %d (encrypted device: %t)\n", session.FacebookUserID, *connectionID, source.E2EEDeviceID != 0)
	return nil
}

// importDeviceKeys copies every whatsmeow table of the plain probe database
// into the encrypted device store. The store is created with the current
// schema first, so both sides share the same columns.
func importDeviceKeys(config *Config, probeDB string) error {
	ctx := context.Background()
	loggers, err := NewLoggers(&Config{LogLevel: config.LogLevel, LibraryLogLevel: config.LibraryLogLevel})
	if err != nil {
		return err
	}
	container, err := OpenDeviceStore(ctx, config, loggers)
	if err != nil {
		return err
	}
	_ = container.Close()

	database, err := sql.Open(sqlCipherDriverName, sqlCipherDSN(config.DeviceDBPath(), config.DeviceKey))
	if err != nil {
		return errors.New("open encrypted device store failed")
	}
	defer database.Close()
	connection, err := database.Conn(ctx)
	if err != nil {
		return errors.New("open encrypted device store failed")
	}
	defer connection.Close()
	attach := fmt.Sprintf("ATTACH DATABASE '%s' AS %s KEY ''", strings.ReplaceAll(probeDB, "'", "''"), attachedProbeSchema)
	if _, err := connection.ExecContext(ctx, attach); err != nil {
		return fmt.Errorf("attach probe database: %w", err)
	}
	defer connection.ExecContext(ctx, "DETACH DATABASE "+attachedProbeSchema)

	tables, err := probeTables(ctx, connection)
	if err != nil {
		return err
	}
	transaction, err := connection.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := transaction.ExecContext(ctx, "PRAGMA defer_foreign_keys = ON"); err != nil {
		_ = transaction.Rollback()
		return err
	}
	for _, tableName := range tables {
		columns, err := tableColumns(ctx, connection, tableName)
		if err != nil {
			_ = transaction.Rollback()
			return err
		}
		columnList := strings.Join(columns, ", ")
		statement := fmt.Sprintf("INSERT OR REPLACE INTO main.%s (%s) SELECT %s FROM %s.%s",
			tableName, columnList, columnList, attachedProbeSchema, tableName)
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			_ = transaction.Rollback()
			return fmt.Errorf("copy %s: %w", tableName, err)
		}
	}
	return transaction.Commit()
}

func probeTables(ctx context.Context, connection *sql.Conn) ([]string, error) {
	rows, err := connection.QueryContext(ctx, fmt.Sprintf(
		"SELECT name FROM %s.sqlite_master WHERE type = 'table' AND name LIKE 'whatsmeow_%%' AND name != '%s'",
		attachedProbeSchema, versionTableName))
	if err != nil {
		return nil, fmt.Errorf("list probe tables: %w", err)
	}
	defer rows.Close()
	tables := []string{deviceTableName}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name != deviceTableName && isPlainIdentifier(name) {
			tables = append(tables, name)
		}
	}
	return tables, rows.Err()
}

func tableColumns(ctx context.Context, connection *sql.Conn, tableName string) ([]string, error) {
	rows, err := connection.QueryContext(ctx, fmt.Sprintf("PRAGMA %s.table_info(%s)", attachedProbeSchema, tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var (
			position, notNull, primaryKey int
			name, columnType              string
			defaultValue                  sql.NullString
		)
		if err := rows.Scan(&position, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, err
		}
		if !isPlainIdentifier(name) {
			return nil, fmt.Errorf("unexpected column name in %s", tableName)
		}
		columns = append(columns, name)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s has no columns", tableName)
	}
	return columns, rows.Err()
}

func isPlainIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for _, character := range name {
		if !(character == '_' || character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9') {
			return false
		}
	}
	return true
}
