package main

import (
	"context"
	"sort"
	"sync"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
)

const (
	profileWait          = 3 * time.Second
	membersWait          = 6 * time.Second
	profileLookupTimeout = 10 * time.Second
	profileRetryAfter    = 10 * time.Minute
	maxChatParticipants  = 20
	maxMemberLookups     = 50
	createThreadSyncGrp  = 1
)

// profileLookups dedupes the profile questions a slot asks Meta: one lookup
// in flight per person or group, and a failed answer is not asked again
// before profileRetryAfter, so a nameless account never costs a task per
// message.
type profileLookups struct {
	mu               sync.Mutex
	contactsInFlight map[int64]chan struct{}
	contactsAsked    map[int64]time.Time
	groupsInFlight   map[int64]chan struct{}
	groupsAsked      map[int64]time.Time
}

func newProfileLookups() *profileLookups {
	return &profileLookups{
		contactsInFlight: make(map[int64]chan struct{}),
		contactsAsked:    make(map[int64]time.Time),
		groupsInFlight:   make(map[int64]chan struct{}),
		groupsAsked:      make(map[int64]time.Time),
	}
}

// prepareProfiles makes sure an inbound event can carry names: the sender,
// the other person of a direct chat, and a group with its members. It waits
// at most profileWait; lookups that take longer finish in the background and
// the next event carries their answer.
func (slot *Slot) prepareProfiles(chat Chat, senderID int64) {
	ctx, cancel := context.WithTimeout(slot.ctx, profileWait)
	defer cancel()
	ids := []int64{senderID}
	if chat.Kind == chatKindDirect {
		ids = append(ids, chat.numericID())
	} else {
		slot.resolveGroup(ctx, chat)
		for _, member := range namedFirst(slot.directory.members(chat.numericID(), slot.selfID()), maxChatParticipants) {
			if member.name == "" {
				ids = append(ids, member.id)
			}
		}
	}
	slot.resolveContacts(ctx, ids)
}

// ChatMembers lists the people of a chat other than the account itself, for
// mentions: a group's members, or the other person of a direct chat.
func (slot *Slot) ChatMembers(ctx context.Context, chat Chat) []memberProfile {
	ctx, cancel := context.WithTimeout(ctx, membersWait)
	defer cancel()
	if chat.Kind == chatKindDirect {
		slot.resolveContacts(ctx, []int64{chat.numericID()})
		contact := slot.directory.contact(chat.numericID())
		return []memberProfile{{id: chat.numericID(), name: contact.name, firstName: contact.firstName, avatarURL: contact.avatarURL}}
	}
	slot.resolveGroup(ctx, chat)
	var nameless []int64
	for _, member := range slot.directory.members(chat.numericID(), slot.selfID()) {
		if member.name == "" && len(nameless) < maxMemberLookups {
			nameless = append(nameless, member.id)
		}
	}
	slot.resolveContacts(ctx, nameless)
	return slot.directory.members(chat.numericID(), slot.selfID())
}

// namedFirst keeps at most limit members, preferring those with a name.
func namedFirst(members []memberProfile, limit int) []memberProfile {
	sort.SliceStable(members, func(i, j int) bool { return members[i].name != "" && members[j].name == "" })
	return members[:min(len(members), limit)]
}

// resolveContacts asks Meta for the people the directory cannot name yet and
// waits for the answers until ctx ends.
func (slot *Slot) resolveContacts(ctx context.Context, ids []int64) {
	selfID := slot.selfID()
	var waits []chan struct{}
	var missing []int64
	lookups := slot.lookups
	lookups.mu.Lock()
	for _, id := range ids {
		if id == 0 || id == selfID || slot.directory.contact(id).name != "" {
			continue
		}
		if inFlight, asking := lookups.contactsInFlight[id]; asking {
			waits = append(waits, inFlight)
			continue
		}
		if time.Since(lookups.contactsAsked[id]) < profileRetryAfter {
			continue
		}
		done := make(chan struct{})
		lookups.contactsInFlight[id] = done
		lookups.contactsAsked[id] = time.Now()
		waits = append(waits, done)
		missing = append(missing, id)
	}
	lookups.mu.Unlock()
	if len(missing) > 0 {
		go slot.lookupContacts(missing)
	}
	for _, done := range waits {
		select {
		case <-done:
		case <-ctx.Done():
			return
		}
	}
}

func (slot *Slot) lookupContacts(ids []int64) {
	asked := false
	defer func() {
		slot.lookups.mu.Lock()
		for _, id := range ids {
			close(slot.lookups.contactsInFlight[id])
			delete(slot.lookups.contactsInFlight, id)
			if !asked {
				delete(slot.lookups.contactsAsked, id)
			}
		}
		slot.lookups.mu.Unlock()
	}()
	client := slot.regularClient()
	if client == nil || !client.IsConnected() {
		return
	}
	asked = true
	ctx, cancel := context.WithTimeout(slot.ctx, profileLookupTimeout)
	defer cancel()
	tasks := make([]socket.Task, len(ids))
	for index, id := range ids {
		tasks[index] = &socket.GetContactsFullTask{ContactID: id}
	}
	response, err := client.ExecuteTasks(ctx, tasks...)
	if err != nil || response == nil {
		slot.log.Warn().Str("event", "contact_lookup_failed").Int("requested", len(ids)).Bool("timed_out", ctx.Err() != nil).Send()
		return
	}
	slot.directory.absorb(response)
	slot.log.Info().Str("event", "contact_lookup").Int("requested", len(ids)).Int("found", len(response.LSDeleteThenInsertContact)).Send()
}

// resolveGroup makes sure the directory knows a group's members, asking Meta
// once when it does not, and waits until they are known or ctx ends. Meta may
// answer in the task response or push the rows afterwards; both land in the
// directory.
func (slot *Slot) resolveGroup(ctx context.Context, chat Chat) {
	groupID := chat.numericID()
	lookups := slot.lookups
	lookups.mu.Lock()
	if _, _, known := slot.directory.groupInfo(groupID); known {
		lookups.mu.Unlock()
		return
	}
	done, asking := lookups.groupsInFlight[groupID]
	if !asking {
		if time.Since(lookups.groupsAsked[groupID]) < profileRetryAfter {
			lookups.mu.Unlock()
			return
		}
		done = make(chan struct{})
		lookups.groupsInFlight[groupID] = done
		lookups.groupsAsked[groupID] = time.Now()
		go slot.lookupGroup(chat, done)
	}
	lookups.mu.Unlock()
	for {
		changed := slot.directory.changes()
		if _, _, known := slot.directory.groupInfo(groupID); known {
			return
		}
		select {
		case <-done:
			done = nil
			lookups.mu.Lock()
			_, sent := lookups.groupsAsked[groupID]
			lookups.mu.Unlock()
			if !sent {
				return
			}
		case <-changed:
		case <-ctx.Done():
			return
		}
	}
}

func (slot *Slot) lookupGroup(chat Chat, done chan struct{}) {
	groupID := chat.numericID()
	defer func() {
		slot.lookups.mu.Lock()
		delete(slot.lookups.groupsInFlight, groupID)
		slot.lookups.mu.Unlock()
		close(done)
	}()
	ctx, cancel := context.WithTimeout(slot.ctx, profileLookupTimeout)
	defer cancel()
	if slot.directory.isEncryptedGroup(groupID) {
		slot.lookupEncryptedGroup(ctx, groupID)
		return
	}
	client := slot.regularClient()
	if client == nil || !client.IsConnected() {
		slot.forgetGroupLookup(groupID)
		return
	}
	response, err := client.ExecuteTasks(ctx, &socket.CreateThreadTask{ThreadFBID: groupID, SyncGroup: createThreadSyncGrp})
	if err != nil || response == nil {
		slot.log.Warn().Str("event", "group_lookup_failed").Int64("group_id", groupID).Bool("timed_out", ctx.Err() != nil).Send()
		return
	}
	slot.directory.absorb(response)
	_, _, known := slot.directory.groupInfo(groupID)
	slot.log.Info().Str("event", "group_lookup").Int64("group_id", groupID).Bool("members_in_response", known).Send()
}

func (slot *Slot) lookupEncryptedGroup(ctx context.Context, groupID int64) {
	client := slot.encryptedClient()
	if client == nil || !client.IsConnected() {
		slot.forgetGroupLookup(groupID)
		return
	}
	info, err := client.GetGroupInfo(ctx, waTypes.NewJID(formatFBID(groupID), waTypes.GroupServer))
	if err != nil || info == nil {
		slot.log.Warn().Str("event", "group_lookup_failed").Int64("group_id", groupID).Bool("encrypted", true).Bool("timed_out", ctx.Err() != nil).Send()
		return
	}
	members := make(map[int64]bool, len(info.Participants))
	for _, participant := range info.Participants {
		if id := jidFBID(participant.JID); id != 0 {
			members[id] = participant.IsAdmin || participant.IsSuperAdmin
		}
	}
	slot.directory.absorbEncryptedGroup(groupID, info.Name, members)
	slot.log.Info().Str("event", "group_lookup").Int64("group_id", groupID).Bool("encrypted", true).Int("members", len(members)).Send()
}

// forgetGroupLookup lets a lookup that could not even be sent run again on
// the next event instead of waiting out profileRetryAfter.
func (slot *Slot) forgetGroupLookup(groupID int64) {
	slot.lookups.mu.Lock()
	delete(slot.lookups.groupsAsked, groupID)
	slot.lookups.mu.Unlock()
}
