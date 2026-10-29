package main

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waArmadilloApplication"
	"go.mau.fi/whatsmeow/proto/waArmadilloXMA"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

const (
	contentKindText        = "text"
	contentKindMedia       = "media"
	contentKindSticker     = "sticker"
	contentKindLocation    = "location"
	contentKindLink        = "link"
	contentKindViewOnce    = "view_once"
	contentKindCall        = "call"
	contentKindSystem      = "system"
	contentKindUnsupported = "unsupported"

	mediaKindImage   = "image"
	mediaKindVideo   = "video"
	mediaKindAudio   = "audio"
	mediaKindFile    = "file"
	mediaKindSticker = "sticker"

	receiptDelivered = "delivered"
	receiptRead      = "read"

	callMediaVideo = "video"

	thumbsUpSmallStickerID  = 369239263222822
	thumbsUpMediumStickerID = 369239343222814
	thumbsUpLargeStickerID  = 369239383222810
	thumbsUpText            = "👍"

	locationShareHost  = "location_share"
	liveLocationCTA    = "xma_live_location_sharing"
	pollCTAPrefix      = "xma_poll_"
	facebookRedirector = "/l.php"
	receiverFetchWait  = 15 * time.Second
)

type mediaItem struct {
	URL         string `json:"url"`
	Kind        string `json:"kind"`
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
}

type locationInfo struct {
	Lat  float64 `json:"lat"`
	Lng  float64 `json:"lng"`
	Name *string `json:"name"`
}

type linkInfo struct {
	URL         string  `json:"url"`
	Title       *string `json:"title"`
	Description *string `json:"description"`
	ImageURL    *string `json:"image_url"`
}

type messageContent struct {
	Kind     string        `json:"kind"`
	Text     *string       `json:"text"`
	ReplyTo  *string       `json:"reply_to"`
	Media    []mediaItem   `json:"media"`
	Location *locationInfo `json:"location"`
	Link     *linkInfo     `json:"link"`
	Mentions []mention     `json:"mentions,omitempty"`
}

func newContent() *messageContent {
	return &messageContent{Media: []mediaItem{}}
}

// settleKind picks the contract kind from what the conversion found, the most
// specific shape winning over plain text.
func (content *messageContent) settleKind() {
	if content.Kind != "" {
		return
	}
	switch {
	case len(content.Media) > 0 && content.Media[0].Kind == mediaKindSticker:
		content.Kind = contentKindSticker
	case len(content.Media) > 0:
		content.Kind = contentKindMedia
	case content.Location != nil:
		content.Kind = contentKindLocation
	case content.Link != nil:
		content.Kind = contentKindLink
	case content.Text != nil:
		content.Kind = contentKindText
	default:
		content.Kind = contentKindUnsupported
	}
}

type inboundMessage struct {
	sourceID  string
	chat      Chat
	senderID  int64
	fromMe    bool
	backfill  bool
	timestamp time.Time
	content   *messageContent
}

// emitInbound runs off the transports' socket read loops (regular job loop,
// encrypted handler queue), so it may wait for profile lookups.
func (slot *Slot) emitInbound(message inboundMessage) error {
	slot.messages.remember(messageRef{
		sourceID: message.sourceID, chat: message.chat, senderID: message.senderID,
		fromMe: message.fromMe, timestampMs: message.timestamp.UnixMilli(),
	})
	slot.prepareProfiles(message.chat, message.senderID)
	event := newWebhookEvent("inbound", slot.connectionID, slot.selfID(), message.timestamp)
	event["source_id"] = message.sourceID
	event["chat"] = slot.chatPayload(message.chat)
	event["sender"] = slot.senderPayload(message.senderID)
	event["from_me"] = message.fromMe
	event["is_request"] = !message.fromMe && slot.directory.isRequest(message.chat)
	event["backfill"] = message.backfill
	event["content"] = message.content
	if err := slot.manager.outbox.Enqueue(event); err != nil {
		slot.log.Error().Str("event", "outbox_enqueue_failed").Str("webhook_type", "inbound").Str("source_id", message.sourceID).Err(err).Send()
		return err
	}
	slot.log.Info().Str("event", "inbound").Str("chat_kind", message.chat.Kind).Str("chat_id", message.chat.ID).
		Str("source_id", message.sourceID).Str("content_kind", message.content.Kind).Bool("from_me", message.fromMe).
		Bool("backfill", message.backfill).Int64("lag_ms", time.Since(message.timestamp).Milliseconds()).Send()
	return nil
}

// chatPayload names the chat: the other person of a direct chat, or a group
// with its own name and picture (null when Meta has none, Laravel then names
// it from the members) and its first members.
func (slot *Slot) chatPayload(chat Chat) map[string]any {
	payload := map[string]any{"kind": chat.Kind, "id": chat.ID}
	if chat.Kind == chatKindDirect {
		if peer := slot.directory.contact(chat.numericID()); peer.name != "" || peer.avatarURL != "" {
			payload["name"] = nullableString(peer.name)
			payload["avatar_url"] = nullableString(peer.avatarURL)
		}
		return payload
	}
	name, picture, _ := slot.directory.groupInfo(chat.numericID())
	members := slot.directory.members(chat.numericID(), slot.selfID())
	participants := make([]map[string]any, 0, min(len(members), maxChatParticipants))
	for _, member := range namedFirst(members, maxChatParticipants) {
		participants = append(participants, member.payload())
	}
	payload["name"] = nullableString(name)
	payload["avatar_url"] = nullableString(picture)
	payload["participants"] = participants
	payload["participant_count"] = len(members)
	return payload
}

func (member memberProfile) payload() map[string]any {
	return map[string]any{
		"id":         formatFBID(member.id),
		"name":       nullableString(member.name),
		"first_name": nullableString(member.firstName),
		"avatar_url": nullableString(member.avatarURL),
		"is_admin":   member.isAdmin,
	}
}

func (slot *Slot) senderPayload(senderID int64) map[string]any {
	name, avatar := "", ""
	if senderID == slot.selfID() {
		slot.mu.Lock()
		name, avatar = slot.selfName, slot.selfAvatarURL
		slot.mu.Unlock()
	} else {
		contact := slot.directory.contact(senderID)
		name, avatar = contact.name, contact.avatarURL
	}
	return map[string]any{"id": formatFBID(senderID), "name": nullableString(name), "avatar_url": nullableString(avatar)}
}

func (slot *Slot) emitEphemeral(eventType string, occurredAt time.Time, fields map[string]any) {
	event := newWebhookEvent(eventType, slot.connectionID, slot.selfID(), occurredAt)
	for name, value := range fields {
		event[name] = value
	}
	slot.manager.outbox.SendNow(event)
}

func (slot *Slot) emitDurable(eventType string, occurredAt time.Time, fields map[string]any) {
	event := newWebhookEvent(eventType, slot.connectionID, slot.selfID(), occurredAt)
	for name, value := range fields {
		event[name] = value
	}
	if err := slot.manager.outbox.Enqueue(event); err != nil {
		slot.log.Error().Str("event", "outbox_enqueue_failed").Str("webhook_type", eventType).Err(err).Send()
	}
}

// ---------- regular transport (Lightspeed tables) ----------

// handleInitialTable absorbs the inbox snapshot. On the very first connect
// it only records a watermark; afterwards it replays what arrived while the
// bridge was offline, flagged as backfill.
func (slot *Slot) handleInitialTable(tbl *table.LSTable) {
	slot.directory.absorb(tbl)
	slot.enqueueRegular(func() { slot.replayInitialTable(tbl) })
}

func (slot *Slot) replayInitialTable(tbl *table.LSTable) {
	slot.mu.Lock()
	watermark := slot.session.LastRegularMessageMs
	slot.mu.Unlock()
	upserts, inserts := tbl.WrapMessages()
	messages := append([]*table.WrappedMessage(nil), inserts...)
	for _, upsert := range upserts {
		messages = append(messages, upsert.Messages...)
	}
	for _, message := range messages {
		if watermark == 0 {
			slot.indexRegularMessage(message)
			slot.advanceRegularWatermark(message.TimestampMs)
			continue
		}
		if message.TimestampMs > watermark {
			slot.handleRegularMessage(message, true)
		}
	}
}

// handleLiveTable runs on the regular socket's read loop: it keeps the
// directory current and relays typing at once, and leaves everything that may
// wait on Meta (profile lookups before an inbound) to the regular job loop.
func (slot *Slot) handleLiveTable(tbl *table.LSTable) {
	slot.scheduleChatUpdates(slot.directory.absorb(tbl))
	for _, typing := range tbl.LSUpdateTypingIndicator {
		if typing.SenderId == slot.selfID() {
			continue
		}
		slot.emitEphemeral("typing", time.Now(), map[string]any{
			"chat":      slot.directory.chatForThread(typing.ThreadKey, typing.SenderId, slot.selfID()),
			"sender_id": formatFBID(typing.SenderId),
			"typing":    typing.IsTyping,
		})
	}
	if client := slot.regularClient(); client != nil {
		client.PostHandlePublishResponse(tbl)
	}
	slot.enqueueRegular(func() { slot.processLiveTable(tbl) })
}

func (slot *Slot) processLiveTable(tbl *table.LSTable) {
	upserts, inserts := tbl.WrapMessages()
	for _, message := range inserts {
		slot.handleRegularMessage(message, false)
	}
	slot.mu.Lock()
	watermark := slot.session.LastRegularMessageMs
	slot.mu.Unlock()
	for _, upsert := range upserts {
		for _, message := range upsert.Messages {
			if message.TimestampMs > watermark {
				slot.handleRegularMessage(message, true)
			}
		}
	}
	slot.handleRegularEdits(tbl.LSEditMessage)
	for _, deleted := range tbl.LSDeleteMessage {
		slot.emitRegularUnsend(deleted.ThreadKey, deleted.MessageId, 0)
	}
	for _, replaced := range tbl.LSDeleteThenInsertMessage {
		if replaced.IsUnsent {
			slot.emitRegularUnsend(replaced.ThreadKey, replaced.MessageId, replaced.SenderId)
		}
	}
	for _, reaction := range tbl.LSUpsertReaction {
		slot.emitRegularReaction(reaction.ThreadKey, reaction.MessageId, reaction.ActorId, reaction.Reaction, reaction.TimestampMs)
	}
	for _, removed := range tbl.LSDeleteReaction {
		slot.emitRegularReaction(removed.ThreadKey, removed.MessageId, removed.ActorId, "", 0)
	}
	for _, receipt := range tbl.LSUpdateReadReceipt {
		slot.emitRegularReadReceipt(receipt)
	}
}

func (slot *Slot) indexRegularMessage(message *table.WrappedMessage) {
	selfID := slot.selfID()
	slot.messages.remember(messageRef{
		sourceID:    regularSourceID(message.MessageId),
		chat:        slot.directory.chatForThread(message.ThreadKey, message.SenderId, selfID),
		senderID:    message.SenderId,
		fromMe:      message.SenderId == selfID,
		timestampMs: message.TimestampMs,
	})
}

func (slot *Slot) handleRegularMessage(message *table.WrappedMessage, backfill bool) {
	if message.MessageId == "" || message.IsUnsent {
		return
	}
	selfID := slot.selfID()
	sourceID := regularSourceID(message.MessageId)
	fromMe := message.SenderId == selfID
	if fromMe && (slot.sentIDs.has(message.OfflineThreadingId) || slot.sentIDs.has(sourceID)) {
		slot.indexRegularMessage(message)
		return
	}
	if !slot.reportedEvents.addIfAbsent(sourceID) {
		return
	}
	chat := slot.directory.chatForThread(message.ThreadKey, message.SenderId, selfID)
	err := slot.emitInbound(inboundMessage{
		sourceID:  sourceID,
		chat:      chat,
		senderID:  message.SenderId,
		fromMe:    fromMe,
		backfill:  backfill,
		timestamp: time.UnixMilli(message.TimestampMs),
		content:   convertRegularMessage(message),
	})
	if err != nil {
		slot.reportedEvents.forget(sourceID)
		return
	}
	slot.advanceRegularWatermark(message.TimestampMs)
}

func convertRegularMessage(message *table.WrappedMessage) *messageContent {
	content := newContent()
	if message.ReplySourceId != "" {
		content.ReplyTo = new(regularSourceID(message.ReplySourceId))
	}
	text := message.Text
	switch message.StickerId {
	case thumbsUpSmallStickerID, thumbsUpMediumStickerID, thumbsUpLargeStickerID:
		if len(message.Stickers) <= 1 {
			text, message.Stickers = thumbsUpText, nil
		}
	}
	if message.IsAdminMessage {
		content.Kind = contentKindSystem
		content.Text = optionalString(text)
		return content
	}
	seenAttachments := make(map[string]bool)
	for _, blob := range message.BlobAttachments {
		if blob.AttachmentFbid != "" && seenAttachments[blob.AttachmentFbid] {
			continue
		}
		seenAttachments[blob.AttachmentFbid] = true
		if blob.AttachmentType == table.AttachmentTypeEphemeralImage || blob.AttachmentType == table.AttachmentTypeEphemeralVideo {
			content.Kind = contentKindViewOnce
			continue
		}
		urlValue, mimeType := blob.PlayableUrl, blob.PlayableUrlMimeType
		if mimeType == "" {
			mimeType = blob.AttachmentMimeType
		}
		if urlValue == "" {
			urlValue, mimeType = blob.PreviewUrl, blob.PreviewUrlMimeType
		}
		content.appendRegularMedia(blob.AttachmentType, urlValue, blob.Filename, mimeType, blob.Filesize)
	}
	for _, legacy := range message.Attachments {
		if legacy.EphemeralMediaViewMode == table.EphemeralMediaViewOnce || legacy.EphemeralMediaViewMode == table.EphemeralMediaReplayable ||
			legacy.AttachmentType == table.AttachmentTypeEphemeralImage || legacy.AttachmentType == table.AttachmentTypeEphemeralVideo {
			content.Kind = contentKindViewOnce
			continue
		}
		urlValue, mimeType := legacy.PlayableUrl, legacy.PlayableUrlMimeType
		if mimeType == "" {
			mimeType = legacy.AttachmentMimeType
		}
		if urlValue == "" {
			urlValue, mimeType = legacy.PreviewUrl, legacy.PreviewUrlMimeType
		}
		content.appendRegularMedia(legacy.AttachmentType, urlValue, legacy.Filename, mimeType, legacy.Filesize)
	}
	for _, sticker := range message.Stickers {
		urlValue, mimeType := sticker.PlayableUrl, sticker.PlayableUrlMimeType
		if urlValue == "" {
			urlValue, mimeType = sticker.PreviewUrl, sticker.PreviewUrlMimeType
		}
		if urlValue != "" {
			content.Media = append(content.Media, mediaItem{URL: urlValue, Kind: mediaKindSticker, Filename: "sticker", ContentType: mimeType})
		}
	}
	for _, card := range message.XMAAttachments {
		content.absorbRegularCard(card)
	}
	if text != "" {
		content.Text = &text
		content.Mentions = regularMentions(text, message.LSInsertMessage)
	}
	if content.Kind == contentKindViewOnce {
		content.Media = []mediaItem{}
	}
	content.settleKind()
	return content
}

func (content *messageContent) appendRegularMedia(attachmentType table.AttachmentType, urlValue, filename, mimeType string, size int64) {
	if urlValue == "" {
		return
	}
	kind := regularMediaKind(attachmentType, mimeType)
	if filename == "" {
		filename = defaultFilename(kind, mimeType)
	}
	content.Media = append(content.Media, mediaItem{URL: urlValue, Kind: kind, Filename: filename, ContentType: mimeType, Size: size})
}

func regularMediaKind(attachmentType table.AttachmentType, mimeType string) string {
	switch attachmentType {
	case table.AttachmentTypeImage, table.AttachmentTypeAnimatedImage:
		return mediaKindImage
	case table.AttachmentTypeVideo:
		return mediaKindVideo
	case table.AttachmentTypeAudio, table.AttachmentTypeSoundBite:
		return mediaKindAudio
	case table.AttachmentTypeSticker, table.AttachmentTypeSelfieSticker, table.AttachmentTypeThirdPartySticker:
		return mediaKindSticker
	}
	return mediaKindFromMime(mimeType)
}

func mediaKindFromMime(mimeType string) string {
	switch {
	case strings.HasPrefix(mimeType, "image/"):
		return mediaKindImage
	case strings.HasPrefix(mimeType, "video/"):
		return mediaKindVideo
	case strings.HasPrefix(mimeType, "audio/"):
		return mediaKindAudio
	}
	return mediaKindFile
}

// absorbRegularCard reads a share card: a location, a poll notice, or a link
// (shared post, reel, article).
func (content *messageContent) absorbRegularCard(card *table.WrappedXMA) {
	if card.CTA != nil && strings.HasPrefix(card.CTA.Type_, pollCTAPrefix) {
		content.Kind = contentKindSystem
		return
	}
	if card.CTA != nil && (card.CTA.Type_ == liveLocationCTA || looksLikeCoordinates(card.CTA.NativeUrl)) {
		if lat, lng, ok := parseCoordinates(card.CTA.NativeUrl); ok {
			content.Location = &locationInfo{Lat: lat, Lng: lng, Name: optionalString(card.TitleText)}
			return
		}
	}
	link := &linkInfo{Title: optionalString(card.TitleText), Description: optionalString(card.SubtitleText), ImageURL: optionalString(card.PreviewUrl)}
	if card.CTA != nil {
		link.URL = unwrapFacebookRedirect(card.CTA.ActionUrl)
	}
	if link.URL == "" {
		link.URL = card.PlayableUrl
	}
	if link.URL == "" && link.Title == nil {
		return
	}
	content.Link = link
}

func looksLikeCoordinates(value string) bool {
	_, _, ok := parseCoordinates(value)
	return ok
}

func parseCoordinates(value string) (float64, float64, bool) {
	latText, lngText, found := strings.Cut(strings.TrimPrefix(value, "geo:"), ",")
	if !found {
		return 0, 0, false
	}
	lat, latErr := strconv.ParseFloat(strings.TrimSpace(latText), 64)
	lng, lngErr := strconv.ParseFloat(strings.TrimSpace(strings.Split(lngText, ";")[0]), 64)
	return lat, lng, latErr == nil && lngErr == nil
}

func unwrapFacebookRedirect(address string) string {
	parsed, err := url.Parse(address)
	if err == nil && parsed.Path == facebookRedirector {
		if target := parsed.Query().Get("u"); target != "" {
			return target
		}
	}
	return address
}

func (slot *Slot) handleRegularEdits(edits []*table.LSEditMessage) {
	for _, edit := range edits {
		sourceID := regularSourceID(edit.MessageID)
		ref, known := slot.messages.lookup(sourceID)
		if !known {
			slot.log.Info().Str("event", "edit_skipped_unknown_message").Str("source_id", sourceID).Send()
			continue
		}
		if !slot.reportedEvents.addIfAbsent("edit:" + sourceID + ":" + strconv.FormatInt(edit.EditCount, 10)) {
			continue
		}
		slot.emitDurable("edit", time.Now(), map[string]any{"chat": ref.chat, "target_source_id": sourceID, "text": edit.Text})
	}
}

func (slot *Slot) emitRegularUnsend(threadKey int64, messageID string, senderID int64) {
	sourceID := regularSourceID(messageID)
	if !slot.reportedEvents.addIfAbsent("unsend:" + sourceID) {
		return
	}
	chat := slot.directory.chatForThread(threadKey, senderID, slot.selfID())
	if ref, known := slot.messages.lookup(sourceID); known {
		chat = ref.chat
		if senderID == 0 {
			senderID = ref.senderID
		}
	}
	slot.emitDurable("unsend", time.Now(), map[string]any{"chat": chat, "target_source_id": sourceID, "sender_id": nullableString(formatFBID(senderID))})
}

func (slot *Slot) emitRegularReaction(threadKey int64, messageID string, actorID int64, emoji string, timestampMs int64) {
	sourceID := regularSourceID(messageID)
	if slot.sentIDs.has(reactionEchoKey(sourceID, emoji)) && actorID == slot.selfID() {
		return
	}
	chat := slot.directory.chatForThread(threadKey, actorID, slot.selfID())
	if ref, known := slot.messages.lookup(sourceID); known {
		chat = ref.chat
	}
	occurredAt := time.Now()
	if timestampMs > 0 {
		occurredAt = time.UnixMilli(timestampMs)
	}
	slot.emitEphemeral("reaction", occurredAt, map[string]any{
		"chat": chat, "target_source_id": sourceID, "sender_id": formatFBID(actorID), "emoji": emoji,
	})
}

func (slot *Slot) emitRegularReadReceipt(receipt *table.LSUpdateReadReceipt) {
	if receipt.ContactId == slot.selfID() {
		return
	}
	chat := slot.directory.chatForThread(receipt.ThreadKey, receipt.ContactId, slot.selfID())
	sourceIDs := slot.messages.ownMessagesReadUpTo(chat, receipt.ReadWatermarkTimestampMs)
	if len(sourceIDs) == 0 {
		return
	}
	slot.emitEphemeral("receipt", time.UnixMilli(receipt.ReadWatermarkTimestampMs), map[string]any{
		"chat": chat, "source_ids": sourceIDs, "status": receiptRead, "by_id": formatFBID(receipt.ContactId),
	})
}

func reactionEchoKey(sourceID, emoji string) string {
	return "reaction:" + sourceID + ":" + emoji
}

// ---------- end-to-end encrypted transport ----------

func (slot *Slot) chatForEncrypted(jid waTypes.JID) (Chat, bool) {
	id, err := strconv.ParseInt(jid.User, 10, 64)
	if err != nil || id == 0 {
		return Chat{}, false
	}
	switch jid.Server {
	case waTypes.MessengerServer, waTypes.DefaultUserServer:
		slot.directory.markEncryptedPeer(id)
		return directChat(id), true
	case waTypes.GroupServer:
		slot.directory.markEncryptedGroup(id)
		return groupChat(id), true
	}
	return Chat{}, false
}

func jidFBID(jid waTypes.JID) int64 {
	id, _ := strconv.ParseInt(jid.User, 10, 64)
	return id
}

// handleEncryptedMessage returns false only when the event could not be
// persisted, so whatsmeow does not acknowledge it and Meta redelivers.
func (slot *Slot) handleEncryptedMessage(event *events.FBMessage) bool {
	chat, ok := slot.chatForEncrypted(event.Info.Chat)
	if !ok {
		return true
	}
	sourceID := encryptedSourceID(event.Info.ID)
	senderID := jidFBID(event.Info.Sender)
	fromMe := event.Info.IsFromMe
	if fromMe {
		senderID = slot.selfID()
	}
	if handled := slot.handleEncryptedAction(event, chat, senderID); handled {
		return true
	}
	if fromMe && slot.sentIDs.has(sourceID) {
		return true
	}
	content := slot.convertEncryptedContent(event)
	if content == nil {
		return true
	}
	if !slot.reportedEvents.addIfAbsent(sourceID) {
		return true
	}
	if qm := event.FBApplication.GetMetadata().GetQuotedMessage(); qm != nil && qm.GetStanzaID() != "" {
		content.ReplyTo = new(encryptedSourceID(qm.GetStanzaID()))
	}
	if !fromMe {
		slot.messages.addPendingRead(chat, event.Info.ID, senderID)
	}
	err := slot.emitInbound(inboundMessage{
		sourceID:  sourceID,
		chat:      chat,
		senderID:  senderID,
		fromMe:    fromMe,
		backfill:  !slot.offlineSyncDone.Load(),
		timestamp: event.Info.Timestamp,
		content:   content,
	})
	if err != nil {
		slot.reportedEvents.forget(sourceID)
		return false
	}
	return true
}

// handleEncryptedAction handles payloads that act on an earlier message
// (reaction, edit, unsend) and device-sync payloads that carry no content.
func (slot *Slot) handleEncryptedAction(event *events.FBMessage, chat Chat, senderID int64) bool {
	switch message := event.Message.(type) {
	case *waConsumerApplication.ConsumerApplication:
		if revoke := message.GetPayload().GetApplicationData().GetRevoke(); revoke != nil {
			target := encryptedSourceID(revoke.GetKey().GetID())
			if slot.reportedEvents.addIfAbsent("unsend:" + target) {
				slot.emitDurable("unsend", event.Info.Timestamp, map[string]any{"chat": chat, "target_source_id": target, "sender_id": formatFBID(senderID)})
			}
			return true
		}
		content := message.GetPayload().GetContent()
		if content == nil {
			return true
		}
		switch typed := content.GetContent().(type) {
		case *waConsumerApplication.ConsumerApplication_Content_ReactionMessage:
			target := encryptedSourceID(typed.ReactionMessage.GetKey().GetID())
			emoji := typed.ReactionMessage.GetText()
			if !(event.Info.IsFromMe && slot.sentIDs.has(reactionEchoKey(target, emoji))) {
				slot.emitEphemeral("reaction", event.Info.Timestamp, map[string]any{
					"chat": chat, "target_source_id": target, "sender_id": formatFBID(senderID), "emoji": emoji,
				})
			}
			return true
		case *waConsumerApplication.ConsumerApplication_Content_EditMessage:
			target := encryptedSourceID(typed.EditMessage.GetKey().GetID())
			if slot.reportedEvents.addIfAbsent("edit:" + target + ":" + strconv.FormatInt(typed.EditMessage.GetTimestampMS(), 10)) {
				slot.emitDurable("edit", event.Info.Timestamp, map[string]any{
					"chat": chat, "target_source_id": target, "text": typed.EditMessage.GetMessage().GetText(),
				})
			}
			return true
		}
	case *waArmadilloApplication.Armadillo:
		if message.GetPayload().GetContent() == nil {
			return true
		}
	case nil:
		return true
	}
	return false
}

func (slot *Slot) convertEncryptedContent(event *events.FBMessage) *messageContent {
	switch message := event.Message.(type) {
	case *waConsumerApplication.ConsumerApplication:
		return slot.convertConsumerContent(message.GetPayload().GetContent())
	case *waArmadilloApplication.Armadillo:
		return slot.convertArmadilloContent(message.GetPayload().GetContent())
	}
	content := newContent()
	content.Kind = contentKindUnsupported
	return content
}

func (slot *Slot) convertConsumerContent(raw *waConsumerApplication.ConsumerApplication_Content) *messageContent {
	content := newContent()
	switch typed := raw.GetContent().(type) {
	case *waConsumerApplication.ConsumerApplication_Content_MessageText:
		slot.absorbEncryptedText(content, typed.MessageText)
	case *waConsumerApplication.ConsumerApplication_Content_ExtendedTextMessage:
		extended := typed.ExtendedTextMessage
		slot.absorbEncryptedText(content, extended.GetText())
		if link := firstNonEmpty(extended.GetCanonicalURL(), extended.GetMatchedText()); link != "" {
			content.Link = &linkInfo{URL: link, Title: optionalString(extended.GetTitle()), Description: optionalString(extended.GetDescription())}
		}
	case *waConsumerApplication.ConsumerApplication_Content_ImageMessage:
		if transport, err := typed.ImageMessage.Decode(); err == nil {
			slot.appendEncryptedMedia(content, transport.GetIntegral().GetTransport(), whatsmeow.MediaImage, mediaKindImage, "")
		}
		slot.absorbEncryptedText(content, typed.ImageMessage.GetCaption())
	case *waConsumerApplication.ConsumerApplication_Content_VideoMessage:
		if transport, err := typed.VideoMessage.Decode(); err == nil {
			slot.appendEncryptedMedia(content, transport.GetIntegral().GetTransport(), whatsmeow.MediaVideo, mediaKindVideo, "")
		}
		slot.absorbEncryptedText(content, typed.VideoMessage.GetCaption())
	case *waConsumerApplication.ConsumerApplication_Content_AudioMessage:
		if transport, err := typed.AudioMessage.Decode(); err == nil {
			slot.appendEncryptedMedia(content, transport.GetIntegral().GetTransport(), whatsmeow.MediaAudio, mediaKindAudio, "")
		}
	case *waConsumerApplication.ConsumerApplication_Content_DocumentMessage:
		if transport, err := typed.DocumentMessage.Decode(); err == nil {
			slot.appendEncryptedMedia(content, transport.GetIntegral().GetTransport(), whatsmeow.MediaDocument, mediaKindFile, typed.DocumentMessage.GetFileName())
		}
	case *waConsumerApplication.ConsumerApplication_Content_StickerMessage:
		slot.appendEncryptedSticker(content, typed.StickerMessage)
	case *waConsumerApplication.ConsumerApplication_Content_ViewOnceMessage:
		content.Kind = contentKindViewOnce
	case *waConsumerApplication.ConsumerApplication_Content_LocationMessage:
		location := typed.LocationMessage.GetLocation()
		content.Location = &locationInfo{Lat: location.GetDegreesLatitude(), Lng: location.GetDegreesLongitude(),
			Name: optionalString(firstNonEmpty(location.GetName(), typed.LocationMessage.GetAddress()))}
	case *waConsumerApplication.ConsumerApplication_Content_LiveLocationMessage:
		location := typed.LiveLocationMessage.GetLocation()
		content.Location = &locationInfo{Lat: location.GetDegreesLatitude(), Lng: location.GetDegreesLongitude(), Name: optionalString(location.GetName())}
	default:
		content.Kind = contentKindUnsupported
	}
	content.settleKind()
	return content
}

func (slot *Slot) convertArmadilloContent(raw *waArmadilloApplication.Armadillo_Content) *messageContent {
	content := newContent()
	switch typed := raw.GetContent().(type) {
	case *waArmadilloApplication.Armadillo_Content_ExtendedContentMessage:
		absorbEncryptedCard(content, typed.ExtendedContentMessage)
	case *waArmadilloApplication.Armadillo_Content_RavenMessage_, *waArmadilloApplication.Armadillo_Content_RavenMessageMsgr:
		content.Kind = contentKindViewOnce
	case *waArmadilloApplication.Armadillo_Content_ImageGalleryMessage_:
		if images, err := typed.ImageGalleryMessage.Decode(); err == nil {
			for _, image := range images {
				slot.appendEncryptedMedia(content, image.GetIntegral().GetTransport(), whatsmeow.MediaImage, mediaKindImage, "")
			}
		}
	default:
		content.Kind = contentKindUnsupported
	}
	content.settleKind()
	return content
}

// absorbEncryptedCard reads Messenger's share card: a location pin, a link or
// shared post, or a story reply.
func absorbEncryptedCard(content *messageContent, card *waArmadilloXMA.ExtendedContentMessage) {
	content.Text = optionalString(card.GetMessageText())
	for _, cta := range card.GetCtas() {
		parsed, err := url.Parse(cta.GetNativeURL())
		if err == nil && parsed.Scheme == "messenger" && parsed.Host == locationShareHost {
			lat, latErr := strconv.ParseFloat(parsed.Query().Get("lat"), 64)
			lng, lngErr := strconv.ParseFloat(parsed.Query().Get("long"), 64)
			if latErr == nil && lngErr == nil {
				content.Location = &locationInfo{Lat: lat, Lng: lng, Name: optionalString(card.GetTitleText())}
				return
			}
		}
	}
	for _, cta := range card.GetCtas() {
		for _, candidate := range []string{cta.GetNativeURL(), cta.GetActionURL()} {
			parsed, err := url.Parse(candidate)
			if err == nil && (parsed.Scheme == "https" || parsed.Scheme == "http") && parsed.Host != "" {
				content.Link = &linkInfo{URL: unwrapFacebookRedirect(candidate), Title: optionalString(card.GetTitleText()),
					Description: optionalString(card.GetSubtitleText())}
				return
			}
		}
	}
	if content.Text == nil && card.GetTitleText() == "" {
		content.Kind = contentKindUnsupported
	}
}

func (slot *Slot) appendEncryptedMedia(content *messageContent, transport *waMediaTransport.WAMediaTransport, mediaType whatsmeow.MediaType, kind, filename string) {
	if transport.GetIntegral().GetDirectPath() == "" {
		return
	}
	contentType := transport.GetAncillary().GetMimetype()
	if contentType == "" {
		contentType = defaultBinaryMimeType
	}
	if kind == mediaKindVideo && strings.HasPrefix(contentType, "image/") {
		kind = mediaKindImage
	}
	if filename == "" {
		filename = defaultFilename(kind, contentType)
	}
	size := int64(transport.GetAncillary().GetFileLength())
	token, err := slot.manager.media.Register(&encryptedMedia{
		connectionID: slot.connectionID, integral: transport.GetIntegral(), mediaType: mediaType,
		contentType: contentType, filename: filename, size: size,
	})
	if err != nil {
		slot.log.Error().Str("event", "media_register_failed").Err(err).Send()
		return
	}
	content.Media = append(content.Media, mediaItem{
		URL: slot.manager.config.PublicURL + "/media/" + token, Kind: kind, Filename: filename, ContentType: contentType, Size: size,
	})
}

// appendEncryptedSticker uses Meta's sticker CDN for catalogue stickers (the
// receiver-fetch id) and the encrypted media path for custom ones.
func (slot *Slot) appendEncryptedSticker(content *messageContent, sticker *waConsumerApplication.ConsumerApplication_StickerMessage) {
	transport, err := sticker.Decode()
	if err != nil {
		return
	}
	if fetchID := transport.GetAncillary().GetReceiverFetchID(); fetchID != "" {
		if client := slot.regularClient(); client != nil {
			ctx, cancel := context.WithTimeout(slot.ctx, receiverFetchWait)
			media, err := client.FetchReceiverFetchMedia(ctx, fetchID)
			cancel()
			if err == nil && media.URL != "" {
				content.Media = append(content.Media, mediaItem{URL: media.URL, Kind: mediaKindSticker, Filename: "sticker", ContentType: media.MimeType})
				return
			}
			slot.log.Warn().Str("event", "sticker_fetch_failed").Err(err).Send()
		}
	}
	slot.appendEncryptedMedia(content, transport.GetIntegral().GetTransport(), whatsmeow.MediaImage, mediaKindSticker, "")
}

func (slot *Slot) handleEncryptedReceipt(receipt *events.Receipt) {
	var status string
	switch receipt.Type {
	case waTypes.ReceiptTypeRead, waTypes.ReceiptTypeReadSelf:
		status = receiptRead
	case waTypes.ReceiptTypeDelivered:
		status = receiptDelivered
	default:
		return
	}
	chat, ok := slot.chatForEncrypted(receipt.Chat)
	if !ok || len(receipt.MessageIDs) == 0 {
		return
	}
	sourceIDs := make([]string, len(receipt.MessageIDs))
	for index, id := range receipt.MessageIDs {
		sourceIDs[index] = encryptedSourceID(id)
	}
	slot.emitEphemeral("receipt", receipt.Timestamp, map[string]any{
		"chat": chat, "source_ids": sourceIDs, "status": status, "by_id": formatFBID(jidFBID(receipt.Sender)),
	})
}

func (slot *Slot) handleEncryptedTyping(presence *events.ChatPresence) {
	chat, ok := slot.chatForEncrypted(presence.Chat)
	if !ok || jidFBID(presence.Sender) == slot.selfID() {
		return
	}
	slot.emitEphemeral("typing", time.Now(), map[string]any{
		"chat": chat, "sender_id": formatFBID(jidFBID(presence.Sender)), "typing": presence.State == waTypes.ChatPresenceComposing,
	})
}

func callMediaFromOffer(offer *events.CallOffer) string {
	if offer.Data != nil && offer.Data.GetChildByTag("video").Tag != "" {
		return callMediaVideo
	}
	return "audio"
}

// handleIncomingCall reports a ringing call once per call id as a call entry
// in the conversation. Answering happens in the official app.
func (slot *Slot) handleIncomingCall(meta waTypes.BasicCallMeta, media string) bool {
	if !meta.GroupJID.IsEmpty() {
		return true
	}
	callerID := jidFBID(meta.CallCreator)
	if callerID == 0 || callerID == slot.selfID() {
		return true
	}
	sourceID := encryptedSourceID("call-" + meta.CallID)
	if !slot.reportedEvents.addIfAbsent(sourceID) {
		return true
	}
	description := "incoming_audio_call"
	if media == callMediaVideo {
		description = "incoming_video_call"
	}
	content := newContent()
	content.Kind = contentKindCall
	content.Text = &description
	timestamp := meta.Timestamp
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	if err := slot.emitInbound(inboundMessage{
		sourceID: sourceID, chat: directChat(callerID), senderID: callerID, timestamp: timestamp, content: content,
		backfill: !slot.offlineSyncDone.Load(),
	}); err != nil {
		slot.reportedEvents.forget(sourceID)
		return false
	}
	return true
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
