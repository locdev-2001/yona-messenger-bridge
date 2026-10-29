package main

import (
	"sync"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const chatUpdateDelay = 2 * time.Second

// chatUpdates coalesces a group's profile changes (name, picture, members)
// into one `chat_updated` webhook per burst, sent only when the group reads
// differently from before the burst.
type chatUpdates struct {
	mu      sync.Mutex
	pending map[int64]*pendingChatUpdate
}

type pendingChatUpdate struct {
	before string
	timer  *time.Timer
}

func newChatUpdates() *chatUpdates {
	return &chatUpdates{pending: make(map[int64]*pendingChatUpdate)}
}

// scheduleChatUpdates takes the groups absorb reported, each with its
// fingerprint from before, and (re)arms their debounce timers.
func (slot *Slot) scheduleChatUpdates(changed map[int64]string) {
	for groupID, before := range changed {
		slot.scheduleChatUpdate(groupID, before)
	}
}

func (slot *Slot) scheduleChatUpdate(groupID int64, before string) {
	updates := slot.chatUpdates
	updates.mu.Lock()
	defer updates.mu.Unlock()
	if pending, waiting := updates.pending[groupID]; waiting {
		pending.timer.Reset(chatUpdateDelay)
		return
	}
	updates.pending[groupID] = &pendingChatUpdate{
		before: before,
		timer:  time.AfterFunc(chatUpdateDelay, func() { slot.emitChatUpdate(groupID) }),
	}
}

func (slot *Slot) emitChatUpdate(groupID int64) {
	updates := slot.chatUpdates
	updates.mu.Lock()
	pending, waiting := updates.pending[groupID]
	delete(updates.pending, groupID)
	updates.mu.Unlock()
	if !waiting || slot.ctx.Err() != nil || slot.selfID() == 0 {
		return
	}
	if slot.directory.groupFingerprint(groupID) == pending.before {
		return
	}
	chat := groupChat(groupID)
	slot.prepareProfiles(chat, 0)
	slot.emitDurable("chat_updated", time.Now(), map[string]any{"chat": slot.chatPayload(chat)})
	slot.log.Info().Str("event", "chat_updated").Str("chat_id", chat.ID).Send()
}

func (slot *Slot) stopChatUpdates() {
	updates := slot.chatUpdates
	updates.mu.Lock()
	defer updates.mu.Unlock()
	for groupID, pending := range updates.pending {
		pending.timer.Stop()
		delete(updates.pending, groupID)
	}
}

// handleEncryptedGroupInfo applies an encrypted group's rename or member
// change pushed by the encrypted transport.
func (slot *Slot) handleEncryptedGroupInfo(info *events.GroupInfo) {
	if info.JID.Server != waTypes.GroupServer || (info.Name == nil && len(info.Join) == 0 && len(info.Leave) == 0) {
		return
	}
	groupID := jidFBID(info.JID)
	if groupID == 0 {
		return
	}
	var name *string
	if info.Name != nil {
		name = &info.Name.Name
	}
	before := slot.directory.applyEncryptedGroupChange(groupID, name, jidIDs(info.Join), jidIDs(info.Leave))
	slot.scheduleChatUpdate(groupID, before)
}

func jidIDs(jids []waTypes.JID) []int64 {
	ids := make([]int64, 0, len(jids))
	for _, jid := range jids {
		if id := jidFBID(jid); id != 0 {
			ids = append(ids, id)
		}
	}
	return ids
}
