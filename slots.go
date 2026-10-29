package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

const selfProfileTimeout = 15 * time.Second

type slotState string

const (
	stateConnecting  slotState = "connecting"
	stateConnected   slotState = "connected"
	stateSessionLost slotState = "session_lost"
	stateAbsent      slotState = "absent"

	sessionStateConnected    = "connected"
	sessionStateLost         = "session_lost"
	sessionStateDisconnected = "disconnected"

	reasonLoggedOut         = "logged_out"
	reasonCheckpoint        = "checkpoint"
	reasonChallenge         = "challenge"
	reasonConsent           = "consent"
	reasonAccountSuspended  = "account_suspended"
	reasonBadCredentials    = "bad_credentials"
	reasonEncryptionLost    = "e2ee_logged_out"
	reasonTemporaryBan      = "temporary_ban"
	reasonReconnectFailed   = "reconnect_failed"
	reasonTransientOutage   = "transient_disconnect"
	reasonSessionUnreadable = "session_unreadable"

	lsErrorBadCredentials = 1357053

	inboxLoadTimeout          = 90 * time.Second
	firstConnectRetryDelay    = 2 * time.Second
	encryptedRetryDelay       = 15 * time.Second
	maxConnectRetryDelay      = 5 * time.Minute
	disconnectReportDelay     = 60 * time.Second
	minFullReconnectInterval  = 30 * time.Second
	maxFullReconnectsInWindow = 5
	fullReconnectWindow       = 30 * time.Minute
	bootRestoreStagger        = 2 * time.Second
	sessionSaveInterval       = time.Minute
	sentIDMemory              = 30 * time.Minute
	reportedEventMemory       = 24 * time.Hour
	regularJobQueueSize       = 1024
)

// Slot is one connected Messenger account: the regular (Lightspeed) client,
// the end-to-end encrypted client on top of it, and the caches both share.
type Slot struct {
	manager      *SlotManager
	connectionID int64
	log          zerolog.Logger
	libraryLog   zerolog.Logger
	ctx          context.Context
	cancel       context.CancelFunc

	mu                  sync.Mutex
	session             *storedSession
	regular             *messagix.Client
	e2ee                *whatsmeow.Client
	device              *store.Device
	state               slotState
	lostReason          string
	regularConnected    bool
	e2eeConnected       bool
	generation          uint64
	announcedConnected  bool
	disconnectTimer     *time.Timer
	fullReconnects      []time.Time
	encryptionResetUsed bool
	sessionDirty        bool
	selfName            string
	selfAvatarURL       string
	presenceAvailable   bool

	e2eeConnectLock sync.Mutex
	offlineSyncDone atomic.Bool
	directory       *chatDirectory
	messages        *messageIndex
	sentIDs         *recentSet
	reportedEvents  *recentSet
	lookups         *profileLookups
	chatUpdates     *chatUpdates
	// regularJobs runs regular-transport message handling off the socket read
	// loop, in order, so handling may wait for answers that loop delivers.
	regularJobs chan func()
}

type SlotManager struct {
	config   *Config
	loggers  *Loggers
	sessions *SessionStore
	devices  *sqlstore.Container
	outbox   *Outbox
	media    *MediaStore
	fetcher  *OutboundFetcher
	ctx      context.Context

	mu    sync.RWMutex
	slots map[int64]*Slot
}

func NewSlotManager(ctx context.Context, config *Config, loggers *Loggers, sessions *SessionStore,
	devices *sqlstore.Container, outbox *Outbox, media *MediaStore) *SlotManager {
	manager := &SlotManager{
		config: config, loggers: loggers, sessions: sessions, devices: devices, outbox: outbox,
		media: media, fetcher: NewOutboundFetcher(config), ctx: ctx, slots: make(map[int64]*Slot),
	}
	media.slots = manager.slot
	return manager
}

func (manager *SlotManager) slot(connectionID int64) *Slot {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	return manager.slots[connectionID]
}

func (manager *SlotManager) ActiveSessionCount() int {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	return len(manager.slots)
}

// HasCapacityFor reports whether a login for this connection may start: a
// connection that already owns a slot can always log in again.
func (manager *SlotManager) HasCapacityFor(connectionID int64, pendingLogins int) bool {
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if _, exists := manager.slots[connectionID]; exists {
		return true
	}
	return len(manager.slots)+pendingLogins < manager.config.MaxSessions
}

func (manager *SlotManager) ConnectedSlot(connectionID int64) (*Slot, error) {
	slot := manager.slot(connectionID)
	if slot == nil {
		return nil, errSlotNotFound
	}
	slot.mu.Lock()
	// A slot riding out one of Meta's routine socket drops still accepts
	// actions; they wait for the socket instead of failing immediately.
	ready := slot.state != stateSessionLost && slot.regular != nil
	slot.mu.Unlock()
	if !ready {
		return nil, errSlotNotReady
	}
	return slot, nil
}

func (manager *SlotManager) Health(connectionID int64) map[string]any {
	health := map[string]any{
		"state": stateAbsent, "regular_connected": false, "e2ee_connected": false,
		"facebook_user_id": nil, "session_lost_reason": nil,
	}
	slot := manager.slot(connectionID)
	if slot == nil {
		return health
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	health["state"] = slot.state
	health["regular_connected"] = slot.regularConnected
	health["e2ee_connected"] = slot.e2eeConnected
	if slot.session != nil && slot.session.FacebookUserID != 0 {
		health["facebook_user_id"] = formatFBID(slot.session.FacebookUserID)
	}
	if slot.lostReason != "" {
		health["session_lost_reason"] = slot.lostReason
	}
	return health
}

// RestoreAll reconnects every saved session, a few seconds apart so a restart
// does not fire dozens of inbox loads from one address at once.
func (manager *SlotManager) RestoreAll() {
	ids, err := manager.sessions.ConnectionIDs()
	if err != nil {
		manager.loggers.App.Error().Str("event", "restore_list_failed").Err(err).Send()
		return
	}
	manager.loggers.App.Info().Str("event", "restore_started").Int("sessions", len(ids)).Send()
	for index, connectionID := range ids {
		if index > 0 && !sleepContext(manager.ctx, bootRestoreStagger) {
			return
		}
		session, err := manager.sessions.Load(connectionID)
		if err != nil {
			manager.loggers.App.Error().Str("event", "restore_session_unreadable").Int64("connection_id", connectionID).Err(err).Send()
			slot := manager.newSlot(connectionID, &storedSession{ConnectionID: connectionID})
			manager.register(slot)
			slot.markLost(reasonSessionUnreadable)
			continue
		}
		slot := manager.newSlot(connectionID, session)
		manager.register(slot)
		go slot.connectLoop(slot.currentGeneration())
	}
}

func (manager *SlotManager) newSlot(connectionID int64, session *storedSession) *Slot {
	ctx, cancel := context.WithCancel(manager.ctx)
	connectionLog := manager.loggers.App.With().Int64("connection_id", connectionID).Logger()
	slot := &Slot{
		manager:        manager,
		connectionID:   connectionID,
		log:            connectionLog,
		libraryLog:     manager.loggers.Library.With().Int64("connection_id", connectionID).Logger(),
		ctx:            ctx,
		cancel:         cancel,
		session:        session,
		state:          stateConnecting,
		directory:      newChatDirectory(),
		messages:       newMessageIndex(),
		sentIDs:        newRecentSet(sentIDMemory),
		reportedEvents: newRecentSet(reportedEventMemory),
		lookups:        newProfileLookups(),
		chatUpdates:    newChatUpdates(),
		regularJobs:    make(chan func(), regularJobQueueSize),
	}
	go slot.runRegularJobs()
	return slot
}

func (slot *Slot) runRegularJobs() {
	for {
		select {
		case job := <-slot.regularJobs:
			job()
		case <-slot.ctx.Done():
			return
		}
	}
}

// enqueueRegular hands work to the regular job loop. A full queue blocks the
// caller, which only delays reading the socket; lookups waiting on it time out.
func (slot *Slot) enqueueRegular(job func()) {
	select {
	case slot.regularJobs <- job:
	case <-slot.ctx.Done():
	}
}

// register puts a slot in place, shutting down whatever slot held the
// connection before (a re-login replaces the previous session).
func (manager *SlotManager) register(slot *Slot) {
	manager.mu.Lock()
	previous := manager.slots[slot.connectionID]
	manager.slots[slot.connectionID] = slot
	manager.mu.Unlock()
	if previous != nil && previous != slot {
		previous.shutdown()
	}
}

// ActivateLogin turns a finished login into a live slot: it loads the inbox
// once synchronously (so the caller gets the account name), persists the
// session and starts listening.
func (manager *SlotManager) ActivateLogin(ctx context.Context, session *storedSession) (*loginAccount, error) {
	previous := manager.slot(session.ConnectionID)
	slot := manager.newSlot(session.ConnectionID, session)
	generation := slot.currentGeneration()
	if err := slot.loadAndConnect(ctx, generation); err != nil {
		slot.cancel()
		if reason, permanent := classifyRegularConnectError(err); permanent {
			slot.log.Warn().Str("event", "login_activation_failed").Str("code", reason).Send()
		} else {
			slot.log.Warn().Str("event", "login_activation_failed").Err(err).Send()
		}
		return nil, errMetaRejected
	}
	manager.register(slot)
	if previous != nil {
		previous.forgetDevice(ctx)
	}
	slot.mu.Lock()
	account := &loginAccount{
		FacebookUserID: formatFBID(slot.session.FacebookUserID),
		DisplayName:    slot.selfName,
		AvatarURL:      nullableString(slot.selfAvatarURL),
	}
	slot.mu.Unlock()
	slot.log.Info().Str("event", "login_activated").Send()
	return account, nil
}

func (manager *SlotManager) Logout(ctx context.Context, connectionID int64) error {
	manager.mu.Lock()
	slot := manager.slots[connectionID]
	delete(manager.slots, connectionID)
	manager.mu.Unlock()
	if slot != nil {
		slot.shutdown()
		slot.forgetDevice(ctx)
	}
	if err := manager.sessions.Delete(connectionID); err != nil {
		return errUnknownFailure
	}
	manager.loggers.App.Info().Str("event", "logout").Int64("connection_id", connectionID).Send()
	return nil
}

// RunSessionSaver persists cookie rotations and the regular-transport
// watermark without writing the session file on every message.
func (manager *SlotManager) RunSessionSaver(ctx context.Context) {
	ticker := time.NewTicker(sessionSaveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			manager.saveDirtySessions()
			return
		case <-ticker.C:
			manager.saveDirtySessions()
		}
	}
}

func (manager *SlotManager) saveDirtySessions() {
	manager.mu.RLock()
	slots := make([]*Slot, 0, len(manager.slots))
	for _, slot := range manager.slots {
		slots = append(slots, slot)
	}
	manager.mu.RUnlock()
	for _, slot := range slots {
		slot.saveSessionIfDirty()
	}
}

func (manager *SlotManager) Shutdown() {
	manager.saveDirtySessions()
	manager.mu.Lock()
	slots := manager.slots
	manager.slots = make(map[int64]*Slot)
	manager.mu.Unlock()
	var wait sync.WaitGroup
	for _, slot := range slots {
		wait.Add(1)
		go func() {
			defer wait.Done()
			slot.shutdown()
		}()
	}
	wait.Wait()
}

func (slot *Slot) currentGeneration() uint64 {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	return slot.generation
}

func (slot *Slot) isCurrent(generation uint64) bool {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	return slot.generation == generation && slot.state != stateSessionLost && slot.ctx.Err() == nil
}

func (slot *Slot) selfID() int64 {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.session == nil {
		return 0
	}
	return slot.session.FacebookUserID
}

func (slot *Slot) regularClient() *messagix.Client {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	return slot.regular
}

func (slot *Slot) encryptedClient() *whatsmeow.Client {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	return slot.e2ee
}

// connectLoop keeps retrying the inbox load and socket connect with capped
// exponential backoff until it succeeds or Meta says the session is gone.
func (slot *Slot) connectLoop(generation uint64) {
	for attempt := 0; ; attempt++ {
		if !slot.isCurrent(generation) {
			return
		}
		err := slot.loadAndConnect(slot.ctx, generation)
		if err == nil {
			return
		}
		if reason, permanent := classifyRegularConnectError(err); permanent {
			slot.log.Warn().Str("event", "regular_connect_rejected").Str("code", reason).Send()
			slot.markLost(reason)
			return
		}
		delay := min(firstConnectRetryDelay<<min(attempt, 10), maxConnectRetryDelay)
		slot.log.Warn().Str("event", "regular_connect_retry").Int("attempt", attempt+1).
			Int64("retry_in_ms", delay.Milliseconds()).Err(err).Send()
		if !sleepContext(slot.ctx, delay) {
			return
		}
	}
}

func (slot *Slot) loadAndConnect(ctx context.Context, generation uint64) error {
	slot.mu.Lock()
	session := slot.session
	slot.mu.Unlock()
	client := messagix.NewClient(session.toCookies(), slot.libraryLog.With().Str("component", "messagix").Logger(), &messagix.Config{})
	if session.NativeSession != nil {
		client.MessengerLite.SetNativeSession(session.NativeSession)
	}
	client.SetEventHandler(func(_ context.Context, event any) { slot.handleRegularEvent(generation, event) })

	loadCtx, cancel := context.WithTimeout(ctx, inboxLoadTimeout)
	startedAt := time.Now()
	currentUser, initialTable, err := client.LoadMessagesPage(loadCtx)
	cancel()
	if err != nil {
		return err
	}
	slot.mu.Lock()
	if slot.generation != generation {
		slot.mu.Unlock()
		return context.Canceled
	}
	slot.regular = client
	slot.session.FacebookUserID = currentUser.GetFBID()
	slot.session.replaceCookies(client.GetCookies())
	slot.selfName = currentUser.GetName()
	slot.selfAvatarURL = currentUser.GetAvatarURL()
	slot.mu.Unlock()
	if err := slot.saveSession(); err != nil {
		slot.log.Error().Str("event", "session_save_failed").Err(err).Send()
	}
	slot.log.Info().Str("event", "inbox_loaded").Int64("duration_ms", time.Since(startedAt).Milliseconds()).Send()
	slot.handleInitialTable(initialTable)
	slot.fillSelfProfileFromDirectory()
	return client.Connect(slot.ctx)
}

func classifyRegularConnectError(err error) (string, bool) {
	var lsError *types.ErrorResponse
	switch {
	case errors.Is(err, httpclient.ErrTokenInvalidated), errors.Is(err, httpclient.ErrUserIDIsZero):
		return reasonLoggedOut, true
	case errors.Is(err, httpclient.ErrCheckpointRequired):
		return reasonCheckpoint, true
	case errors.Is(err, httpclient.ErrChallengeRequired):
		return reasonChallenge, true
	case errors.Is(err, httpclient.ErrConsentRequired):
		return reasonConsent, true
	case errors.Is(err, httpclient.ErrAccountSuspended):
		return reasonAccountSuspended, true
	case errors.As(err, &lsError) && lsError.ErrorCode == lsErrorBadCredentials:
		return reasonBadCredentials, true
	}
	return "", false
}

func (slot *Slot) handleRegularEvent(generation uint64, event any) {
	if !slot.isCurrent(generation) {
		return
	}
	switch typed := event.(type) {
	case *messagix.ConnectedEvent:
		slot.log.Info().Str("event", "regular_connected").Send()
		slot.setRegularConnected(true)
		go slot.connectEncrypted(generation)
	case *messagix.ReconnectedEvent:
		slot.log.Info().Str("event", "regular_reconnected").Send()
		slot.setRegularConnected(true)
		if slot.encryptedClient() == nil {
			go slot.connectEncrypted(generation)
		}
	case *messagix.TransientDisconnectEvent:
		slot.log.Warn().Str("event", "regular_transient_disconnect").Int("attempt", typed.ConnectionAttempts).Err(typed.Err).Send()
		slot.setRegularConnected(false)
	case *messagix.PermanentErrorEvent:
		if websocket.CloseStatus(typed.Err) == dgw.CloseStatusUnauthorized {
			slot.log.Warn().Str("event", "regular_unauthorized").Send()
			slot.markLost(reasonLoggedOut)
			return
		}
		slot.log.Warn().Str("event", "regular_permanent_error").Err(typed.Err).Send()
		slot.requestFullReconnect("regular_permanent_error")
	case *table.LSTable:
		slot.handleLiveTable(typed)
	}
}

// connectEncrypted attaches the end-to-end encrypted transport: load or
// register this account's device, then connect with handlers attached first.
func (slot *Slot) connectEncrypted(generation uint64) {
	for attempt := 0; ; attempt++ {
		err := slot.tryConnectEncrypted(generation)
		if err == nil || !slot.isCurrent(generation) {
			return
		}
		delay := min(encryptedRetryDelay<<min(attempt, 4), maxConnectRetryDelay)
		slot.log.Warn().Str("event", "e2ee_connect_failed").Int("attempt", attempt+1).
			Int64("retry_in_ms", delay.Milliseconds()).Err(err).Send()
		if !sleepContext(slot.ctx, delay) {
			return
		}
	}
}

func (slot *Slot) tryConnectEncrypted(generation uint64) error {
	slot.e2eeConnectLock.Lock()
	defer slot.e2eeConnectLock.Unlock()
	if !slot.isCurrent(generation) || slot.encryptedClient() != nil {
		return nil
	}
	return slot.attachEncryptedClient(generation)
}

func (slot *Slot) attachEncryptedClient(generation uint64) error {
	ctx := slot.ctx
	slot.mu.Lock()
	regular := slot.regular
	session := slot.session
	device := slot.device
	slot.mu.Unlock()
	if regular == nil {
		return errors.New("regular client is not ready")
	}
	if device == nil && session.E2EEDeviceID != 0 {
		deviceJID := waTypes.JID{User: strconv.FormatInt(session.FacebookUserID, 10), Device: session.E2EEDeviceID, Server: waTypes.MessengerServer}
		loaded, err := slot.manager.devices.GetDevice(ctx, deviceJID)
		if err != nil {
			return fmt.Errorf("load device: %w", err)
		}
		device = loaded
		if device == nil {
			slot.log.Warn().Str("event", "e2ee_device_missing").Send()
		}
	}
	isNewDevice := device == nil
	if isNewDevice {
		device = slot.manager.devices.NewDevice()
		if session.NativeSession != nil {
			device.FacebookUUID = session.NativeSession.DeviceID
		}
	}
	regular.SetDevice(device)
	if isNewDevice {
		slot.log.Info().Str("event", "e2ee_device_registering").Send()
		if err := regular.RegisterE2EE(ctx, session.FacebookUserID); err != nil {
			return fmt.Errorf("register device: %w", err)
		}
		if err := device.Save(ctx); err != nil {
			return fmt.Errorf("save device: %w", err)
		}
		slot.mu.Lock()
		slot.session.E2EEDeviceID = device.ID.Device
		slot.mu.Unlock()
		if err := slot.saveSession(); err != nil {
			return fmt.Errorf("remember device: %w", err)
		}
	}
	client, err := regular.PrepareE2EEClient()
	if err != nil {
		return fmt.Errorf("prepare client: %w", err)
	}
	client.SynchronousAck = true
	client.EnableDecryptedEventBuffer = true
	client.AddEventHandlerWithSuccessStatus(func(event any) bool { return slot.handleEncryptedEvent(generation, event) })
	slot.mu.Lock()
	if slot.generation != generation {
		slot.mu.Unlock()
		return context.Canceled
	}
	slot.device = device
	slot.e2ee = client
	slot.mu.Unlock()
	slot.offlineSyncDone.Store(false)
	if err := client.ConnectContext(ctx); err != nil {
		slot.dropEncryptedClient()
		return fmt.Errorf("connect: %w", err)
	}
	return nil
}

func (slot *Slot) handleEncryptedEvent(generation uint64, event any) bool {
	if !slot.isCurrent(generation) {
		_, isMessage := event.(*events.FBMessage)
		return !isMessage
	}
	switch typed := event.(type) {
	case *events.Connected:
		slot.log.Info().Str("event", "e2ee_connected").Send()
		slot.mu.Lock()
		slot.encryptionResetUsed = false
		slot.mu.Unlock()
		slot.setEncryptedConnected(true)
	case *events.Disconnected:
		slot.log.Warn().Str("event", "e2ee_disconnected").Send()
		slot.setEncryptedConnected(false)
	case *events.OfflineSyncCompleted:
		slot.offlineSyncDone.Store(true)
		slot.log.Info().Str("event", "e2ee_offline_sync_completed").Int("count", typed.Count).Send()
	case *events.CATRefreshError:
		slot.log.Warn().Str("event", "e2ee_cat_refresh_error").Err(typed.Error).Send()
		slot.requestFullReconnect("e2ee_cat_refresh_error")
	case *events.LoggedOut:
		slot.handleEncryptionLoggedOut(typed)
	case *events.ConnectFailure:
		slot.handleEncryptionConnectFailure(generation, typed)
	case *events.TemporaryBan:
		slot.log.Warn().Str("event", "e2ee_temporary_ban").Str("code", typed.Code.String()).Send()
		slot.markLost(reasonTemporaryBan)
	case events.PermanentDisconnect:
		slot.log.Warn().Str("event", "e2ee_permanent_disconnect").Str("type", fmt.Sprintf("%T", typed)).Send()
		slot.requestFullReconnect("e2ee_permanent_disconnect")
	case *events.FBMessage:
		return slot.handleEncryptedMessage(typed)
	case *events.Receipt:
		slot.handleEncryptedReceipt(typed)
	case *events.ChatPresence:
		slot.handleEncryptedTyping(typed)
	case *events.GroupInfo:
		slot.handleEncryptedGroupInfo(typed)
	case *events.CallOffer:
		return slot.handleIncomingCall(typed.BasicCallMeta, callMediaFromOffer(typed))
	case *events.CallOfferNotice:
		return slot.handleIncomingCall(typed.BasicCallMeta, typed.Media)
	case *events.UndecryptableMessage:
		slot.log.Warn().Str("event", "e2ee_undecryptable").Str("source_id", encryptedSourceID(typed.Info.ID)).Send()
	}
	return true
}

func (slot *Slot) handleEncryptionLoggedOut(event *events.LoggedOut) {
	slot.mu.Lock()
	canReset := !event.OnConnect && !slot.encryptionResetUsed
	slot.encryptionResetUsed = true
	slot.mu.Unlock()
	slot.log.Warn().Str("event", "e2ee_logged_out").Str("code", event.Reason.String()).Bool("will_reset", canReset).Send()
	if !canReset {
		slot.markLost(reasonEncryptionLost)
		return
	}
	slot.resetEncryptionDevice(slot.ctx)
	slot.requestFullReconnect("e2ee_logged_out")
}

func (slot *Slot) handleEncryptionConnectFailure(generation uint64, event *events.ConnectFailure) {
	slot.log.Warn().Str("event", "e2ee_connect_failure").Str("code", event.Reason.String()).Send()
	switch event.Reason {
	case events.ConnectFailureNotFound:
		slot.dropEncryptedClient()
		slot.resetEncryptionDevice(slot.ctx)
		go slot.connectEncrypted(generation)
	case events.ConnectFailureClientUnknown:
		slot.resetEncryptionDevice(slot.ctx)
		slot.requestFullReconnect("e2ee_client_unknown")
	default:
		slot.requestFullReconnect("e2ee_connect_failure")
	}
}

func (slot *Slot) dropEncryptedClient() {
	slot.mu.Lock()
	client := slot.e2ee
	slot.e2ee = nil
	slot.e2eeConnected = false
	slot.mu.Unlock()
	if client != nil {
		client.Disconnect()
	}
}

// resetEncryptionDevice forgets the registered encrypted device so the next
// connect registers a fresh one.
func (slot *Slot) resetEncryptionDevice(ctx context.Context) {
	slot.mu.Lock()
	device := slot.device
	slot.device = nil
	if slot.session != nil {
		slot.session.E2EEDeviceID = 0
	}
	slot.mu.Unlock()
	if device != nil && device.ID != nil {
		if err := device.Delete(ctx); err != nil {
			slot.log.Warn().Str("event", "e2ee_device_delete_failed").Err(err).Send()
		}
	}
	if err := slot.saveSession(); err != nil {
		slot.log.Error().Str("event", "session_save_failed").Err(err).Send()
	}
}

// forgetDevice removes the encrypted device keys of this slot from disk.
func (slot *Slot) forgetDevice(ctx context.Context) {
	slot.mu.Lock()
	device := slot.device
	session := slot.session
	slot.mu.Unlock()
	if device == nil && session != nil && session.E2EEDeviceID != 0 && session.FacebookUserID != 0 {
		deviceJID := waTypes.JID{User: strconv.FormatInt(session.FacebookUserID, 10), Device: session.E2EEDeviceID, Server: waTypes.MessengerServer}
		device, _ = slot.manager.devices.GetDevice(ctx, deviceJID)
	}
	if device != nil && device.ID != nil {
		if err := device.Delete(ctx); err != nil {
			slot.log.Warn().Str("event", "e2ee_device_delete_failed").Err(err).Send()
		}
	}
}

// requestFullReconnect tears both transports down and connects again from
// the inbox load, at most a few times per window before giving up.
func (slot *Slot) requestFullReconnect(cause string) {
	slot.mu.Lock()
	if slot.state == stateSessionLost || slot.ctx.Err() != nil {
		slot.mu.Unlock()
		return
	}
	now := time.Now()
	recent := slot.fullReconnects[:0]
	for _, at := range slot.fullReconnects {
		if now.Sub(at) < fullReconnectWindow {
			recent = append(recent, at)
		}
	}
	slot.fullReconnects = recent
	if len(recent) > 0 && now.Sub(recent[len(recent)-1]) < minFullReconnectInterval {
		slot.mu.Unlock()
		return
	}
	if len(recent) >= maxFullReconnectsInWindow {
		slot.mu.Unlock()
		slot.log.Warn().Str("event", "full_reconnect_exhausted").Str("cause", cause).Send()
		slot.markLost(reasonReconnectFailed)
		return
	}
	slot.fullReconnects = append(slot.fullReconnects, now)
	slot.generation++
	generation := slot.generation
	regular, encrypted := slot.regular, slot.e2ee
	slot.regular, slot.e2ee = nil, nil
	slot.regularConnected, slot.e2eeConnected = false, false
	slot.state = stateConnecting
	slot.mu.Unlock()
	slot.log.Info().Str("event", "full_reconnect").Str("cause", cause).Send()
	go func() {
		disconnectClients(regular, encrypted)
		slot.scheduleDisconnectReport()
		slot.connectLoop(generation)
	}()
}

func disconnectClients(regular *messagix.Client, encrypted *whatsmeow.Client) {
	if encrypted != nil {
		encrypted.Disconnect()
	}
	if regular != nil {
		regular.SetEventHandler(nil)
		regular.Disconnect()
	}
}

func (slot *Slot) setRegularConnected(connected bool) {
	slot.mu.Lock()
	slot.regularConnected = connected
	slot.mu.Unlock()
	slot.refreshState()
}

func (slot *Slot) setEncryptedConnected(connected bool) {
	slot.mu.Lock()
	slot.e2eeConnected = connected
	slot.mu.Unlock()
	slot.refreshState()
}

// refreshState derives the slot state from both transports. The regular
// transport carries the account, so it alone decides connected; Meta's
// routine socket drops recover in seconds and are only reported to Laravel
// when they last longer than disconnectReportDelay.
func (slot *Slot) refreshState() {
	slot.mu.Lock()
	if slot.state == stateSessionLost {
		slot.mu.Unlock()
		return
	}
	if slot.regularConnected {
		slot.state = stateConnected
		if slot.disconnectTimer != nil {
			slot.disconnectTimer.Stop()
			slot.disconnectTimer = nil
		}
		shouldAnnounce := !slot.announcedConnected
		slot.announcedConnected = true
		slot.mu.Unlock()
		if shouldAnnounce {
			go slot.announceConnected()
		}
		return
	}
	slot.state = stateConnecting
	slot.mu.Unlock()
	slot.scheduleDisconnectReport()
}

func (slot *Slot) scheduleDisconnectReport() {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.disconnectTimer != nil || !slot.announcedConnected {
		return
	}
	slot.disconnectTimer = time.AfterFunc(disconnectReportDelay, func() {
		slot.mu.Lock()
		slot.disconnectTimer = nil
		stillDown := slot.state == stateConnecting && slot.ctx.Err() == nil
		if stillDown {
			slot.announcedConnected = false
		}
		slot.mu.Unlock()
		if stillDown {
			slot.log.Warn().Str("event", "session_disconnected").Send()
			slot.emitSessionEvent(sessionStateDisconnected, reasonTransientOutage)
		}
	})
}

// markLost stops both transports and keeps the slot so Laravel can show why
// and the user can log in again into the same connection.
func (slot *Slot) markLost(reason string) {
	slot.mu.Lock()
	if slot.state == stateSessionLost {
		slot.mu.Unlock()
		return
	}
	slot.state = stateSessionLost
	slot.lostReason = reason
	slot.generation++
	slot.regularConnected, slot.e2eeConnected = false, false
	slot.announcedConnected = false
	regular, encrypted := slot.regular, slot.e2ee
	slot.regular, slot.e2ee = nil, nil
	if slot.disconnectTimer != nil {
		slot.disconnectTimer.Stop()
		slot.disconnectTimer = nil
	}
	slot.mu.Unlock()
	slot.log.Warn().Str("event", "session_lost").Str("code", reason).Send()
	go disconnectClients(regular, encrypted)
	slot.emitSessionEvent(sessionStateLost, reason)
}

func (slot *Slot) shutdown() {
	slot.mu.Lock()
	slot.generation++
	regular, encrypted := slot.regular, slot.e2ee
	slot.regular, slot.e2ee = nil, nil
	if slot.disconnectTimer != nil {
		slot.disconnectTimer.Stop()
		slot.disconnectTimer = nil
	}
	slot.mu.Unlock()
	slot.cancel()
	slot.stopChatUpdates()
	disconnectClients(regular, encrypted)
}

func (slot *Slot) emitSessionEvent(state, reason string) {
	event := newWebhookEvent("session", slot.connectionID, slot.selfID(), time.Now())
	event["state"] = state
	event["reason"] = nullableString(reason)
	if state == sessionStateConnected {
		slot.mu.Lock()
		event["account"] = map[string]any{
			"display_name": nullableString(slot.selfName),
			"avatar_url":   nullableString(slot.selfAvatarURL),
		}
		slot.mu.Unlock()
	}
	if err := slot.manager.outbox.Enqueue(event); err != nil {
		slot.log.Error().Str("event", "outbox_enqueue_failed").Str("webhook_type", "session").Err(err).Send()
	}
}

func (slot *Slot) saveSession() error {
	slot.mu.Lock()
	snapshot := *slot.session
	slot.sessionDirty = false
	slot.mu.Unlock()
	return slot.manager.sessions.Save(&snapshot)
}

func (slot *Slot) saveSessionIfDirty() {
	slot.mu.Lock()
	dirty := slot.sessionDirty && slot.session != nil && slot.session.FacebookUserID != 0
	slot.mu.Unlock()
	if !dirty {
		return
	}
	if err := slot.saveSession(); err != nil {
		slot.log.Error().Str("event", "session_save_failed").Err(err).Send()
	}
}

func (slot *Slot) advanceRegularWatermark(timestampMs int64) {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.session != nil && timestampMs > slot.session.LastRegularMessageMs {
		slot.session.LastRegularMessageMs = timestampMs
		slot.sessionDirty = true
	}
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

var errNotConnected = newAPIError(http.StatusConflict, codeSessionLost, "The transport needed for this chat is not connected.")

// announceConnected tells Laravel the session is up, with the account's picture
// when Meta answers in time; the announcement never waits longer than that.
func (slot *Slot) announceConnected() {
	fetched := make(chan struct{})
	go func() {
		slot.fetchSelfProfile()
		close(fetched)
	}()
	select {
	case <-fetched:
	case <-time.After(selfProfileTimeout):
		slot.log.Warn().Str("event", "self_profile_timeout").Send()
	}
	slot.emitSessionEvent(sessionStateConnected, "")
}

// fetchSelfProfile asks Meta for the account's own contact row: the current-user
// payload of the inbox page carries no picture, so the inbox would stay faceless.
func (slot *Slot) fetchSelfProfile() {
	slot.mu.Lock()
	hasAvatar := slot.selfAvatarURL != ""
	selfID := slot.session.FacebookUserID
	slot.mu.Unlock()
	client := slot.regularClient()
	if hasAvatar || client == nil || selfID == 0 {
		slot.log.Info().Str("event", "self_profile_skipped").Bool("has_avatar", hasAvatar).Bool("has_client", client != nil).Bool("has_self_id", selfID != 0).Send()
		return
	}
	ctx, cancel := context.WithTimeout(slot.ctx, selfProfileTimeout)
	defer cancel()
	response, err := client.ExecuteTasks(ctx, &socket.GetContactsFullTask{ContactID: selfID})
	if err != nil || response == nil || len(response.LSDeleteThenInsertContact) == 0 {
		slot.log.Warn().Str("event", "self_profile_unavailable").Bool("request_failed", err != nil).Send()
		return
	}
	contact := response.LSDeleteThenInsertContact[0]
	slot.mu.Lock()
	slot.selfAvatarURL = contact.GetAvatarURL()
	if slot.selfName == "" {
		slot.selfName = contact.GetName()
	}
	hasAvatar = slot.selfAvatarURL != ""
	slot.mu.Unlock()
	slot.log.Info().Str("event", "self_profile_loaded").Bool("has_avatar", hasAvatar).Send()
}

// fillSelfProfileFromDirectory covers the native-app login, whose current-user
// payload carries no picture: the inbox snapshot lists the account itself as a
// contact with its avatar.
func (slot *Slot) fillSelfProfileFromDirectory() {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	self := slot.directory.contact(slot.session.FacebookUserID)
	if slot.selfAvatarURL == "" {
		slot.selfAvatarURL = self.avatarURL
	}
	if slot.selfName == "" {
		slot.selfName = self.name
	}
}
