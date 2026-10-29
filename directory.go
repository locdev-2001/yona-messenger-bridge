package main

import (
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

const (
	folderPending          = "pending"
	messageRequestPending  = 1
	recentMessagesPerChat  = 200
	maxIndexedChats        = 2000
	maxPendingReadsPerChat = 100
)

type threadInfo struct {
	threadType table.ThreadType
	name       string
	pictureURL string
	pending    bool
	// participants is nil until Meta listed the members of a group thread.
	participants map[int64]*participantInfo
}

type participantInfo struct {
	nickname string
	isAdmin  bool
}

type contactInfo struct {
	name      string
	firstName string
	avatarURL string
}

// memberProfile is one group member as the contract shows it.
type memberProfile struct {
	id        int64
	name      string
	firstName string
	avatarURL string
	isAdmin   bool
}

// chatDirectory is the per-account view of threads and contacts, seeded from
// the inbox snapshot and kept current from every regular-transport table. It
// answers routing questions (is this 1-1 encrypted? is it a request?) and
// gives inbound events their names and avatars.
type chatDirectory struct {
	mu             sync.RWMutex
	threads        map[int64]*threadInfo
	contacts       map[int64]contactInfo
	jidByFBKey     map[int64]int64
	fbKeyByJID     map[int64]int64
	encryptedPeer  map[int64]bool
	encryptedGroup map[int64]bool
	// changed is closed and replaced whenever absorb learns something, so a
	// lookup can wait for rows Meta pushes after answering its task.
	changed chan struct{}
}

func newChatDirectory() *chatDirectory {
	return &chatDirectory{
		threads:        make(map[int64]*threadInfo),
		contacts:       make(map[int64]contactInfo),
		jidByFBKey:     make(map[int64]int64),
		fbKeyByJID:     make(map[int64]int64),
		encryptedPeer:  make(map[int64]bool),
		encryptedGroup: make(map[int64]bool),
		changed:        make(chan struct{}),
	}
}

// changes returns a channel closed on the next absorb.
func (directory *chatDirectory) changes() <-chan struct{} {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	return directory.changed
}

// absorb records a table and returns the groups whose name, picture or
// members the table may have changed, each with its fingerprint from before.
func (directory *chatDirectory) absorb(tbl *table.LSTable) map[int64]string {
	directory.mu.Lock()
	defer directory.mu.Unlock()
	for _, mapping := range tbl.LSUpdateThreadAuthorityAndMappingWithOTIDFromJID {
		directory.mapHybridLocked(mapping.ThreadKey, mapping.ThreadJID)
	}
	for _, hybrid := range tbl.LSVerifyHybridThreadExists {
		directory.mapHybridLocked(hybrid.ThreadKey, hybrid.ThreadJID)
		directory.threadLocked(hybrid.ThreadJID).threadType = hybrid.ThreadType
		directory.recordEncryptedTypeLocked(hybrid.ThreadJID, hybrid.ThreadType)
	}
	before := make(map[int64]string)
	for _, key := range directory.profileKeysLocked(tbl) {
		before[key] = directory.groupFingerprintLocked(key)
	}
	for _, thread := range tbl.LSDeleteThenInsertThread {
		directory.recordThreadLocked(thread.ThreadKey, thread.ThreadType, thread.ThreadName, thread.ThreadPictureUrl, thread.FolderName)
	}
	for _, thread := range tbl.LSUpdateOrInsertThread {
		directory.recordThreadLocked(thread.ThreadKey, thread.ThreadType, thread.ThreadName, thread.ThreadPictureUrl, thread.FolderName)
	}
	for _, request := range tbl.LSDeleteThenInsertMessageRequest {
		directory.threadLocked(directory.mappedKeyLocked(request.ThreadKey)).pending = request.MessageRequestStatus == messageRequestPending
	}
	for _, rename := range tbl.LSSyncUpdateThreadName {
		directory.threadLocked(directory.mappedKeyLocked(rename.ThreadKey)).name = rename.ThreadName
	}
	for _, contact := range tbl.LSDeleteThenInsertContact {
		directory.recordContactLocked(contact)
	}
	for _, contact := range tbl.LSVerifyContactRowExists {
		directory.recordContactLocked(contact)
	}
	for _, participant := range tbl.LSVerifyContactParticipantExist {
		if existing := directory.contacts[participant.ContactID]; existing.name == "" && participant.Name != "" && participant.ContactID != 0 {
			existing.name = participant.Name
			directory.contacts[participant.ContactID] = existing
		}
	}
	for _, image := range tbl.LSSetThreadImageURL {
		directory.threadLocked(directory.mappedKeyLocked(image.ThreadKey)).pictureURL = image.ImageURL
	}
	directory.absorbParticipantsLocked(tbl)
	close(directory.changed)
	directory.changed = make(chan struct{})
	for key := range before {
		if !directory.isGroupLocked(key) {
			delete(before, key)
		}
	}
	return before
}

// profileKeysLocked lists the threads whose shown profile a table touches.
func (directory *chatDirectory) profileKeysLocked(tbl *table.LSTable) []int64 {
	var keys []int64
	for _, thread := range tbl.LSDeleteThenInsertThread {
		keys = append(keys, thread.ThreadKey)
	}
	for _, thread := range tbl.LSUpdateOrInsertThread {
		keys = append(keys, thread.ThreadKey)
	}
	for _, rename := range tbl.LSSyncUpdateThreadName {
		keys = append(keys, rename.ThreadKey)
	}
	for _, image := range tbl.LSSetThreadImageURL {
		keys = append(keys, image.ThreadKey)
	}
	for _, removedAll := range tbl.LSRemoveAllParticipantsForThread {
		keys = append(keys, removedAll.ThreadKey)
	}
	for _, added := range tbl.LSAddParticipantIdToGroupThread {
		keys = append(keys, added.ThreadKey)
	}
	for _, removed := range tbl.LSRemoveParticipantFromThread {
		keys = append(keys, removed.ThreadKey)
	}
	for index, key := range keys {
		keys[index] = directory.mappedKeyLocked(key)
	}
	return keys
}

func (directory *chatDirectory) isGroupLocked(key int64) bool {
	thread, known := directory.threads[key]
	if !known {
		return false
	}
	if thread.threadType != table.UNKNOWN_THREAD_TYPE {
		return !thread.threadType.IsOneToOne()
	}
	return thread.participants != nil
}

// groupFingerprint summarises what Yona shows of a group: its name, its
// picture (by path, Meta re-signs the same picture's URL) and its members.
func (directory *chatDirectory) groupFingerprint(key int64) string {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	return directory.groupFingerprintLocked(key)
}

func (directory *chatDirectory) groupFingerprintLocked(key int64) string {
	thread, known := directory.threads[key]
	if !known {
		return ""
	}
	var builder strings.Builder
	builder.WriteString(thread.name)
	builder.WriteByte(0)
	builder.WriteString(urlPath(thread.pictureURL))
	builder.WriteByte(0)
	if thread.participants == nil {
		return builder.String()
	}
	ids := make([]int64, 0, len(thread.participants))
	for id := range thread.participants {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		builder.WriteString(strconv.FormatInt(id, 10))
		builder.WriteByte(':')
		builder.WriteString(thread.participants[id].nickname)
		builder.WriteByte(0)
	}
	return builder.String()
}

// urlPath drops the host too: the same picture is served from many CDN hosts.
func urlPath(address string) string {
	if parsed, err := url.Parse(address); err == nil {
		return parsed.Path
	}
	return address
}

// absorbParticipantsLocked keeps group membership current. A full resync
// removes everyone first, then lists the members again.
func (directory *chatDirectory) absorbParticipantsLocked(tbl *table.LSTable) {
	for _, removedAll := range tbl.LSRemoveAllParticipantsForThread {
		directory.threadLocked(directory.mappedKeyLocked(removedAll.ThreadKey)).participants = make(map[int64]*participantInfo)
	}
	for _, added := range tbl.LSAddParticipantIdToGroupThread {
		if added.ContactId == 0 {
			continue
		}
		thread := directory.threadLocked(directory.mappedKeyLocked(added.ThreadKey))
		if thread.participants == nil {
			thread.participants = make(map[int64]*participantInfo)
		}
		thread.participants[added.ContactId] = &participantInfo{nickname: added.Nickname, isAdmin: added.IsAdmin || added.IsSuperAdmin}
	}
	for _, removed := range tbl.LSRemoveParticipantFromThread {
		if thread, known := directory.threads[directory.mappedKeyLocked(removed.ThreadKey)]; known && thread.participants != nil {
			delete(thread.participants, removed.ParticipantId)
		}
	}
	for _, admin := range tbl.LSUpdateThreadParticipantAdminStatus {
		if thread, known := directory.threads[directory.mappedKeyLocked(admin.ThreadKey)]; known && thread.participants != nil {
			if participant, member := thread.participants[admin.ContactId]; member {
				participant.isAdmin = admin.IsAdmin
			}
		}
	}
	for _, overwrite := range tbl.LSOverwriteAllThreadParticipantsAdminStatus {
		if thread, known := directory.threads[directory.mappedKeyLocked(overwrite.ThreadKey)]; known {
			for _, participant := range thread.participants {
				participant.isAdmin = overwrite.IsAdmin
			}
		}
	}
}

// absorbEncryptedGroup records an encrypted group's name and members from its
// group info; member names still come from the contact rows.
func (directory *chatDirectory) absorbEncryptedGroup(groupID int64, name string, members map[int64]bool) {
	directory.mu.Lock()
	defer directory.mu.Unlock()
	thread := directory.threadLocked(groupID)
	thread.threadType = table.ENCRYPTED_OVER_WA_GROUP
	if name != "" {
		thread.name = name
	}
	previous := thread.participants
	thread.participants = make(map[int64]*participantInfo, len(members))
	for id, isAdmin := range members {
		participant := &participantInfo{isAdmin: isAdmin}
		if known, existed := previous[id]; existed {
			participant.nickname = known.nickname
		}
		thread.participants[id] = participant
	}
	directory.encryptedGroup[groupID] = true
	close(directory.changed)
	directory.changed = make(chan struct{})
}

// applyEncryptedGroupChange records an encrypted group's info change as the
// encrypted transport pushes it, and returns the group's fingerprint from
// before the change.
func (directory *chatDirectory) applyEncryptedGroupChange(groupID int64, name *string, joined, left []int64) string {
	directory.mu.Lock()
	defer directory.mu.Unlock()
	before := directory.groupFingerprintLocked(groupID)
	thread := directory.threadLocked(groupID)
	thread.threadType = table.ENCRYPTED_OVER_WA_GROUP
	directory.encryptedGroup[groupID] = true
	if name != nil {
		thread.name = *name
	}
	if thread.participants != nil {
		for _, id := range joined {
			if _, member := thread.participants[id]; !member {
				thread.participants[id] = &participantInfo{}
			}
		}
		for _, id := range left {
			delete(thread.participants, id)
		}
	}
	close(directory.changed)
	directory.changed = make(chan struct{})
	return before
}

func (directory *chatDirectory) mapHybridLocked(fbKey, jidKey int64) {
	if fbKey == 0 || jidKey == 0 {
		return
	}
	directory.jidByFBKey[fbKey] = jidKey
	directory.fbKeyByJID[jidKey] = fbKey
}

func (directory *chatDirectory) mappedKeyLocked(threadKey int64) int64 {
	if jidKey, mapped := directory.jidByFBKey[threadKey]; mapped {
		return jidKey
	}
	return threadKey
}

func (directory *chatDirectory) threadLocked(key int64) *threadInfo {
	thread, exists := directory.threads[key]
	if !exists {
		thread = &threadInfo{}
		directory.threads[key] = thread
	}
	return thread
}

func (directory *chatDirectory) recordThreadLocked(threadKey int64, threadType table.ThreadType, name, pictureURL, folder string) {
	key := directory.mappedKeyLocked(threadKey)
	thread := directory.threadLocked(key)
	if threadType != table.UNKNOWN_THREAD_TYPE {
		thread.threadType = threadType
	}
	if name != "" {
		thread.name = name
	}
	if pictureURL != "" {
		thread.pictureURL = pictureURL
	}
	if folder != "" {
		thread.pending = folder == folderPending
	}
	directory.recordEncryptedTypeLocked(key, threadType)
}

func (directory *chatDirectory) recordEncryptedTypeLocked(key int64, threadType table.ThreadType) {
	switch threadType {
	case table.ENCRYPTED_OVER_WA_ONE_TO_ONE:
		directory.encryptedPeer[key] = true
	case table.ENCRYPTED_OVER_WA_GROUP:
		directory.encryptedGroup[key] = true
	}
}

func (directory *chatDirectory) recordContactLocked(contact types.UserInfo) {
	if contact.GetFBID() == 0 {
		return
	}
	existing := directory.contacts[contact.GetFBID()]
	if name := contact.GetName(); name != "" {
		existing.name = name
	}
	switch typed := contact.(type) {
	case *table.LSDeleteThenInsertContact:
		existing.firstName = firstNonEmpty(typed.FirstName, existing.firstName)
	case *table.LSVerifyContactRowExists:
		existing.firstName = firstNonEmpty(typed.FirstName, existing.firstName)
	}
	if avatar := contact.GetAvatarURL(); avatar != "" {
		existing.avatarURL = avatar
	}
	directory.contacts[contact.GetFBID()] = existing
}

func (directory *chatDirectory) markEncryptedPeer(fbid int64) {
	directory.mu.Lock()
	directory.encryptedPeer[fbid] = true
	directory.mu.Unlock()
}

func (directory *chatDirectory) markEncryptedGroup(groupID int64) {
	directory.mu.Lock()
	directory.encryptedGroup[groupID] = true
	directory.mu.Unlock()
}

// isEncryptedGroup reports a group that lives on the encrypted transport, so
// sends and actions for it must go there instead of the regular thread.
func (directory *chatDirectory) isEncryptedGroup(groupID int64) bool {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	return directory.encryptedGroup[groupID]
}

// chatForThread names the contract chat for a regular-transport thread. A
// one-to-one thread key equals the other person's fbid.
func (directory *chatDirectory) chatForThread(threadKey, senderID, selfID int64) Chat {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	key := directory.mappedKeyLocked(threadKey)
	if thread, known := directory.threads[key]; known && thread.threadType != table.UNKNOWN_THREAD_TYPE {
		if thread.threadType.IsOneToOne() {
			return directChat(key)
		}
		return groupChat(key)
	}
	if _, isContact := directory.contacts[key]; isContact || (key == senderID && senderID != selfID) {
		return directChat(key)
	}
	return groupChat(key)
}

// isRegularOneToOne reports that a direct chat is known to live only on the
// regular transport (pages, legacy chats), so sends must not go encrypted.
func (directory *chatDirectory) isRegularOneToOne(peerID int64) bool {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	if directory.encryptedPeer[peerID] {
		return false
	}
	thread, known := directory.threads[peerID]
	return known && thread.threadType != table.UNKNOWN_THREAD_TYPE && !thread.threadType.IsWhatsApp() && thread.threadType.IsOneToOne()
}

func (directory *chatDirectory) isKnownEncryptedPeer(peerID int64) bool {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	return directory.encryptedPeer[peerID]
}

func (directory *chatDirectory) isRequest(chat Chat) bool {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	thread, known := directory.threads[chat.numericID()]
	return known && thread.pending
}

func (directory *chatDirectory) markAccepted(chat Chat) {
	directory.mu.Lock()
	defer directory.mu.Unlock()
	if thread, known := directory.threads[chat.numericID()]; known {
		thread.pending = false
	}
}

// requestThreadKey is the regular-transport key used to accept a request;
// encrypted one-to-one threads are accepted by their original Facebook key.
func (directory *chatDirectory) requestThreadKey(chat Chat) int64 {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	if fbKey, mapped := directory.fbKeyByJID[chat.numericID()]; mapped {
		return fbKey
	}
	return chat.numericID()
}

func (directory *chatDirectory) threadType(chat Chat) table.ThreadType {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	if thread, known := directory.threads[chat.numericID()]; known {
		return thread.threadType
	}
	if chat.Kind == chatKindDirect {
		return table.ONE_TO_ONE
	}
	return table.GROUP_THREAD
}

func (directory *chatDirectory) contact(fbid int64) contactInfo {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	return directory.contacts[fbid]
}

// groupInfo returns a group's own name and picture (empty when Meta has none)
// and whether its member list is known.
func (directory *chatDirectory) groupInfo(groupID int64) (string, string, bool) {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	thread, known := directory.threads[groupID]
	if !known {
		return "", "", false
	}
	return thread.name, thread.pictureURL, thread.participants != nil
}

// members lists a group's members other than the account itself, admins
// first and then by id so the order is stable between calls.
func (directory *chatDirectory) members(groupID, selfID int64) []memberProfile {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	thread, known := directory.threads[groupID]
	if !known {
		return nil
	}
	members := make([]memberProfile, 0, len(thread.participants))
	for id, participant := range thread.participants {
		if id == selfID {
			continue
		}
		contact := directory.contacts[id]
		members = append(members, memberProfile{
			id: id, name: firstNonEmpty(participant.nickname, contact.name), firstName: firstNonEmpty(participant.nickname, contact.firstName),
			avatarURL: contact.avatarURL, isAdmin: participant.isAdmin,
		})
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].isAdmin != members[j].isAdmin {
			return members[i].isAdmin
		}
		return members[i].id < members[j].id
	})
	return members
}

// messageRef is what the bridge remembers about a recent message so actions
// that only carry a source_id (react, unsend, reply, receipts) can be routed.
type messageRef struct {
	sourceID    string
	chat        Chat
	senderID    int64
	fromMe      bool
	timestampMs int64
	readByPeer  bool
}

type chatMessages struct {
	refs         []*messageRef
	pendingReads []pendingRead
	lastTouched  time.Time
}

type pendingRead struct {
	stanzaID string
	senderID int64
}

// messageIndex is a bounded in-memory index of recent messages per chat.
type messageIndex struct {
	mu       sync.Mutex
	bySource map[string]*messageRef
	byChat   map[Chat]*chatMessages
}

func newMessageIndex() *messageIndex {
	return &messageIndex{bySource: make(map[string]*messageRef), byChat: make(map[Chat]*chatMessages)}
}

func (index *messageIndex) chatLocked(chat Chat) *chatMessages {
	messages, exists := index.byChat[chat]
	if !exists {
		if len(index.byChat) >= maxIndexedChats {
			index.evictOldestChatLocked()
		}
		messages = &chatMessages{}
		index.byChat[chat] = messages
	}
	messages.lastTouched = time.Now()
	return messages
}

func (index *messageIndex) evictOldestChatLocked() {
	var oldestChat Chat
	var oldestTime time.Time
	for chat, messages := range index.byChat {
		if oldestTime.IsZero() || messages.lastTouched.Before(oldestTime) {
			oldestChat, oldestTime = chat, messages.lastTouched
		}
	}
	for _, ref := range index.byChat[oldestChat].refs {
		delete(index.bySource, ref.sourceID)
	}
	delete(index.byChat, oldestChat)
}

func (index *messageIndex) remember(ref messageRef) {
	index.mu.Lock()
	defer index.mu.Unlock()
	if existing, known := index.bySource[ref.sourceID]; known {
		existing.timestampMs = max(existing.timestampMs, ref.timestampMs)
		return
	}
	messages := index.chatLocked(ref.chat)
	stored := &ref
	messages.refs = append(messages.refs, stored)
	sort.SliceStable(messages.refs, func(i, j int) bool { return messages.refs[i].timestampMs < messages.refs[j].timestampMs })
	if len(messages.refs) > recentMessagesPerChat {
		for _, evicted := range messages.refs[:len(messages.refs)-recentMessagesPerChat] {
			delete(index.bySource, evicted.sourceID)
		}
		messages.refs = append([]*messageRef(nil), messages.refs[len(messages.refs)-recentMessagesPerChat:]...)
	}
	index.bySource[ref.sourceID] = stored
}

func (index *messageIndex) lookup(sourceID string) (messageRef, bool) {
	index.mu.Lock()
	defer index.mu.Unlock()
	if ref, known := index.bySource[sourceID]; known {
		return *ref, true
	}
	return messageRef{}, false
}

// ownMessagesReadUpTo returns own messages in a chat up to a read watermark
// that were not yet reported as read, marking them reported.
func (index *messageIndex) ownMessagesReadUpTo(chat Chat, watermarkMs int64) []string {
	index.mu.Lock()
	defer index.mu.Unlock()
	messages, known := index.byChat[chat]
	if !known {
		return nil
	}
	var sourceIDs []string
	for _, ref := range messages.refs {
		if ref.fromMe && !ref.readByPeer && ref.timestampMs <= watermarkMs {
			ref.readByPeer = true
			sourceIDs = append(sourceIDs, ref.sourceID)
		}
	}
	return sourceIDs
}

func (index *messageIndex) addPendingRead(chat Chat, stanzaID string, senderID int64) {
	index.mu.Lock()
	defer index.mu.Unlock()
	messages := index.chatLocked(chat)
	messages.pendingReads = append(messages.pendingReads, pendingRead{stanzaID: stanzaID, senderID: senderID})
	if len(messages.pendingReads) > maxPendingReadsPerChat {
		messages.pendingReads = messages.pendingReads[len(messages.pendingReads)-maxPendingReadsPerChat:]
	}
}

func (index *messageIndex) takePendingReads(chat Chat) []pendingRead {
	index.mu.Lock()
	defer index.mu.Unlock()
	messages, known := index.byChat[chat]
	if !known {
		return nil
	}
	reads := messages.pendingReads
	messages.pendingReads = nil
	return reads
}

// recentSet remembers ids for a short time: ids this bridge just sent (to
// drop their echoes) and events already reported once.
type recentSet struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]time.Time
}

func newRecentSet(ttl time.Duration) *recentSet {
	return &recentSet{ttl: ttl, entries: make(map[string]time.Time)}
}

func (set *recentSet) add(id string) {
	if id == "" {
		return
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	now := time.Now()
	set.entries[id] = now
	if len(set.entries)%256 == 0 {
		for key, addedAt := range set.entries {
			if now.Sub(addedAt) > set.ttl {
				delete(set.entries, key)
			}
		}
	}
}

func (set *recentSet) has(id string) bool {
	if id == "" {
		return false
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	addedAt, exists := set.entries[id]
	return exists && time.Since(addedAt) <= set.ttl
}

func (set *recentSet) forget(id string) {
	set.mu.Lock()
	delete(set.entries, id)
	set.mu.Unlock()
}

// addIfAbsent records id and reports whether it was new.
func (set *recentSet) addIfAbsent(id string) bool {
	set.mu.Lock()
	defer set.mu.Unlock()
	if addedAt, exists := set.entries[id]; exists && time.Since(addedAt) <= set.ttl {
		return false
	}
	set.entries[id] = time.Now()
	return true
}
