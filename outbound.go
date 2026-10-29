package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	_ "image/gif"
	_ "image/jpeg"
	"image/png"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	armadillo "go.mau.fi/whatsmeow/proto"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	waTypes "go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/methods"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

const (
	waitForRegularSocket   = 10 * time.Second
	maxRegularSendAttempts = 3
	thumbnailMaxSide       = 400
	gifMimeType            = "image/gif"
	pngMimeType            = "image/png"
	messageRequestSyncGrp  = 95
	defaultSyncGroup       = 1
)

type SendMedia struct {
	URL         string `json:"url"`
	Kind        string `json:"kind"`
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
}

type SendRequest struct {
	MessageID    int64      `json:"message_id"`
	ConnectionID int64      `json:"connection_id"`
	Chat         Chat       `json:"chat"`
	Text         *string    `json:"text"`
	Media        *SendMedia `json:"media"`
	ReplyTo      *string    `json:"reply_to"`
	// Mentions mark people inside Text, in UTF-16 code units of Text.
	Mentions []mention `json:"mentions"`
}

type sendResult struct {
	sourceID string
	timings  map[string]int64
}

type transportRoute int

const (
	routeRegular transportRoute = iota
	routeEncrypted
)

// routeFor decides which transport carries a chat: direct chats go
// end-to-end encrypted unless the peer is known to be regular-only or has no
// encrypted devices; groups use their regular thread unless they are
// encrypted groups.
func (slot *Slot) routeFor(ctx context.Context, chat Chat) (transportRoute, error) {
	if chat.Kind == chatKindGroup {
		if slot.directory.isEncryptedGroup(chat.numericID()) {
			return routeEncrypted, nil
		}
		return routeRegular, nil
	}
	if slot.directory.isRegularOneToOne(chat.numericID()) {
		return routeRegular, nil
	}
	client := slot.encryptedClient()
	if client == nil || !client.IsConnected() {
		return routeEncrypted, errNotConnected
	}
	if slot.directory.isKnownEncryptedPeer(chat.numericID()) {
		return routeEncrypted, nil
	}
	devices, err := client.GetUserDevicesContext(ctx, []waTypes.JID{encryptedChatJID(chat)})
	if err == nil && len(devices) == 0 {
		return routeRegular, nil
	}
	return routeEncrypted, nil
}

func encryptedChatJID(chat Chat) waTypes.JID {
	if chat.Kind == chatKindGroup {
		return waTypes.NewJID(chat.ID, waTypes.GroupServer)
	}
	return waTypes.NewJID(chat.ID, waTypes.MessengerServer)
}

func (slot *Slot) Send(ctx context.Context, request *SendRequest) (*sendResult, error) {
	mentions, err := validateMentions(request.Chat, stringValue(request.Text), request.Mentions)
	if err != nil {
		return nil, err
	}
	var media *fetchedMedia
	if request.Media != nil {
		fetched, err := slot.manager.fetcher.Fetch(ctx, request.Media)
		if err != nil {
			return nil, err
		}
		media = fetched
	}
	route, err := slot.routeFor(ctx, request.Chat)
	if err != nil {
		return nil, err
	}
	var result *sendResult
	if route == routeEncrypted {
		result, err = slot.sendEncrypted(ctx, request, media, mentions)
	} else {
		result, err = slot.sendRegular(ctx, request, media, mentions)
	}
	if err != nil {
		return nil, err
	}
	slot.messages.remember(messageRef{sourceID: result.sourceID, chat: request.Chat, senderID: slot.selfID(), fromMe: true, timestampMs: time.Now().UnixMilli()})
	slot.log.Info().Str("event", "sent").Int64("message_id", request.MessageID).Str("chat_kind", request.Chat.Kind).
		Str("chat_id", request.Chat.ID).Str("source_id", result.sourceID).Bool("has_media", media != nil).Int("mentions", len(mentions)).
		Int64("devices_ms", result.timings["devices"]).Int64("encrypt_ms", result.timings["encrypt"]).
		Int64("ack_ms", result.timings["ack"]).Send()
	return result, nil
}

// ---------- encrypted transport ----------

func (slot *Slot) sendEncrypted(ctx context.Context, request *SendRequest, media *fetchedMedia, mentions []outboundMention) (*sendResult, error) {
	client := slot.encryptedClient()
	if client == nil {
		return nil, errNotConnected
	}
	caption := encryptedMentionText(stringValue(request.Text), mentions)
	var content waConsumerApplication.ConsumerApplication_Content
	if media != nil {
		wrapped, err := slot.uploadEncryptedMedia(ctx, client, request.Media.Kind, media, caption)
		if err != nil {
			return nil, err
		}
		content.Content = wrapped
	} else {
		content.Content = &waConsumerApplication.ConsumerApplication_Content_MessageText{MessageText: caption}
	}
	message := &waConsumerApplication.ConsumerApplication{
		Payload: &waConsumerApplication.ConsumerApplication_Payload{
			Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{Content: &content},
		},
	}
	metadata := &waMsgApplication.MessageApplication_Metadata{}
	if request.ReplyTo != nil && *request.ReplyTo != "" {
		quoted, err := slot.quotedEncryptedMessage(request.Chat, *request.ReplyTo)
		if err != nil {
			return nil, err
		}
		metadata.QuotedMessage = quoted
	}
	messageID := strconv.FormatInt(methods.GenerateEpochID(), 10)
	slot.sentIDs.add(encryptedSourceID(messageID))
	response, err := client.SendFBMessage(ctx, encryptedChatJID(request.Chat), message, metadata, whatsmeow.SendRequestExtra{ID: messageID})
	if err != nil {
		return nil, wrapMetaError(err)
	}
	timings := response.DebugTimings
	return &sendResult{
		sourceID: encryptedSourceID(messageID),
		timings: map[string]int64{
			"devices": timings.GetDevices.Milliseconds(),
			"encrypt": (timings.PeerEncrypt + timings.GroupEncrypt).Milliseconds(),
			"ack":     (timings.Send + timings.Resp).Milliseconds(),
		},
	}, nil
}

func (slot *Slot) quotedEncryptedMessage(chat Chat, replyTo string) (*waMsgApplication.MessageApplication_Metadata_QuotedMessage, error) {
	parsed, err := parseSourceID(replyTo)
	if err != nil || !parsed.encrypted {
		return nil, errBadRequest
	}
	participant := chat.numericID()
	if ref, known := slot.messages.lookup(replyTo); known {
		participant = ref.senderID
	}
	return &waMsgApplication.MessageApplication_Metadata_QuotedMessage{
		StanzaID:    proto.String(parsed.id),
		Participant: proto.String(waTypes.NewJID(formatFBID(participant), waTypes.MessengerServer).String()),
	}, nil
}

// uploadEncryptedMedia encrypts and uploads an attachment, then wraps it the
// way Messenger clients expect (thumbnail size is mandatory on iOS).
func (slot *Slot) uploadEncryptedMedia(ctx context.Context, client *whatsmeow.Client, kind string, media *fetchedMedia,
	caption *waCommon.MessageText) (waConsumerApplication.ConsumerApplication_Content_Content, error) {
	if kind == "" {
		kind = mediaKindFromMime(media.contentType)
	}
	data, width, height := prepareImage(kind, media)
	if kind == mediaKindImage && media.contentType == gifMimeType {
		kind = mediaKindVideo
	}
	mediaType := whatsmeowMediaType(kind)
	uploaded, err := client.Upload(ctx, data, mediaType)
	if err != nil {
		return nil, wrapMetaError(err)
	}
	thumbWidth, thumbHeight := clampThumbnail(width, height)
	if thumbWidth == 0 && kind == mediaKindImage {
		thumbWidth, thumbHeight = thumbnailMaxSide, thumbnailMaxSide
	}
	transport := &waMediaTransport.WAMediaTransport{
		Integral: &waMediaTransport.WAMediaTransport_Integral{
			FileSHA256:        uploaded.FileSHA256,
			MediaKey:          uploaded.MediaKey,
			FileEncSHA256:     uploaded.FileEncSHA256,
			DirectPath:        &uploaded.DirectPath,
			MediaKeyTimestamp: proto.Int64(time.Now().Unix()),
		},
		Ancillary: &waMediaTransport.WAMediaTransport_Ancillary{
			FileLength: proto.Uint64(uint64(len(data))),
			Mimetype:   proto.String(media.contentType),
			Thumbnail: &waMediaTransport.WAMediaTransport_Ancillary_Thumbnail{
				ThumbnailWidth:  proto.Uint32(uint32(thumbWidth)),
				ThumbnailHeight: proto.Uint32(uint32(thumbHeight)),
			},
			ObjectID: &uploaded.ObjectID,
		},
	}
	switch kind {
	case mediaKindImage:
		message := &waConsumerApplication.ConsumerApplication_ImageMessage{Caption: caption}
		err = message.Set(&waMediaTransport.ImageTransport{
			Integral:  &waMediaTransport.ImageTransport_Integral{Transport: transport},
			Ancillary: &waMediaTransport.ImageTransport_Ancillary{Width: proto.Uint32(uint32(width)), Height: proto.Uint32(uint32(height))},
		})
		return &waConsumerApplication.ConsumerApplication_Content_ImageMessage{ImageMessage: message}, err
	case mediaKindVideo:
		message := &waConsumerApplication.ConsumerApplication_VideoMessage{Caption: caption}
		ancillary := &waMediaTransport.VideoTransport_Ancillary{Width: proto.Uint32(uint32(width)), Height: proto.Uint32(uint32(height))}
		if media.contentType == gifMimeType {
			ancillary.GifPlayback = proto.Bool(true)
		}
		err = message.Set(&waMediaTransport.VideoTransport{
			Integral:  &waMediaTransport.VideoTransport_Integral{Transport: transport},
			Ancillary: ancillary,
		})
		return &waConsumerApplication.ConsumerApplication_Content_VideoMessage{VideoMessage: message}, err
	case mediaKindAudio:
		message := &waConsumerApplication.ConsumerApplication_AudioMessage{PTT: proto.Bool(false)}
		err = message.Set(&waMediaTransport.AudioTransport{
			Integral:  &waMediaTransport.AudioTransport_Integral{Transport: transport},
			Ancillary: &waMediaTransport.AudioTransport_Ancillary{},
		})
		return &waConsumerApplication.ConsumerApplication_Content_AudioMessage{AudioMessage: message}, err
	default:
		filename := media.filename
		if filename == "" {
			filename = defaultFilename(mediaKindFile, media.contentType)
		}
		message := &waConsumerApplication.ConsumerApplication_DocumentMessage{FileName: proto.String(filename)}
		err = message.Set(&waMediaTransport.DocumentTransport{
			Integral:  &waMediaTransport.DocumentTransport_Integral{Transport: transport},
			Ancillary: &waMediaTransport.DocumentTransport_Ancillary{},
		})
		return &waConsumerApplication.ConsumerApplication_Content_DocumentMessage{DocumentMessage: message}, err
	}
}

func whatsmeowMediaType(kind string) whatsmeow.MediaType {
	switch kind {
	case mediaKindImage:
		return whatsmeow.MediaImage
	case mediaKindVideo:
		return whatsmeow.MediaVideo
	case mediaKindAudio:
		return whatsmeow.MediaAudio
	}
	return whatsmeow.MediaDocument
}

// prepareImage reads image dimensions and re-encodes PNGs Messenger refuses
// to render (anything that is not NRGBA).
func prepareImage(kind string, media *fetchedMedia) ([]byte, int, int) {
	if kind != mediaKindImage {
		return media.data, 0, 0
	}
	config, _, err := image.DecodeConfig(bytes.NewReader(media.data))
	if err != nil {
		return media.data, 0, 0
	}
	if media.contentType == pngMimeType && config.ColorModel != color.NRGBAModel {
		if decoded, err := png.Decode(bytes.NewReader(media.data)); err == nil {
			bounds := decoded.Bounds()
			converted := image.NewNRGBA(image.Rect(0, 0, bounds.Dx(), bounds.Dy()))
			draw.Draw(converted, converted.Bounds(), decoded, bounds.Min, draw.Src)
			var encoded bytes.Buffer
			if png.Encode(&encoded, converted) == nil {
				return encoded.Bytes(), config.Width, config.Height
			}
		}
	}
	return media.data, config.Width, config.Height
}

func clampThumbnail(width, height int) (int, int) {
	if width > thumbnailMaxSide {
		height = height * thumbnailMaxSide / width
		width = thumbnailMaxSide
	}
	if height > thumbnailMaxSide {
		width = width * thumbnailMaxSide / height
		height = thumbnailMaxSide
	}
	return width, height
}

func wrapConsumerContent(content waConsumerApplication.ConsumerApplication_Content_Content) *waConsumerApplication.ConsumerApplication {
	return &waConsumerApplication.ConsumerApplication{
		Payload: &waConsumerApplication.ConsumerApplication_Payload{
			Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{
				Content: &waConsumerApplication.ConsumerApplication_Content{Content: content},
			},
		},
	}
}

func (slot *Slot) encryptedTargetKey(chat Chat, target parsedSourceID, sourceID string, assumeFromMe bool) *waCommon.MessageKey {
	key := &waCommon.MessageKey{RemoteJID: proto.String(encryptedChatJID(chat).String()), ID: proto.String(target.id)}
	ref, known := slot.messages.lookup(sourceID)
	fromMe := assumeFromMe
	if known {
		fromMe = ref.fromMe
	}
	if fromMe {
		key.FromMe = proto.Bool(true)
	}
	if chat.Kind == chatKindGroup && known {
		key.Participant = proto.String(waTypes.NewJID(formatFBID(ref.senderID), waTypes.MessengerServer).String())
	}
	return key
}

func (slot *Slot) sendEncryptedAction(ctx context.Context, chat Chat, message armadillo.RealMessageApplicationSub) error {
	client := slot.encryptedClient()
	if client == nil {
		return errNotConnected
	}
	_, err := client.SendFBMessage(ctx, encryptedChatJID(chat), message, nil, whatsmeow.SendRequestExtra{
		ID: strconv.FormatInt(methods.GenerateEpochID(), 10),
	})
	return wrapMetaError(err)
}

// ---------- regular transport ----------

func (slot *Slot) readyRegularClient(ctx context.Context) (*messagix.Client, error) {
	client := slot.regularClient()
	if client == nil {
		return nil, errNotConnected
	}
	if err := client.WaitUntilCanSendMessages(ctx, waitForRegularSocket); err != nil {
		return nil, errNotConnected
	}
	return client, nil
}

func (slot *Slot) sendRegular(ctx context.Context, request *SendRequest, media *fetchedMedia, mentions []outboundMention) (*sendResult, error) {
	client, err := slot.readyRegularClient(ctx)
	if err != nil {
		return nil, err
	}
	threadID := request.Chat.numericID()
	otid := methods.GenerateEpochID()
	otidText := strconv.FormatInt(otid, 10)
	task := &socket.SendMessageTask{
		ThreadId:         threadID,
		Otid:             otid,
		Source:           table.MESSENGER_INBOX_IN_THREAD,
		InitiatingSource: table.FACEBOOK_INBOX,
		SendType:         table.TEXT,
		SyncGroup:        defaultSyncGroup,
		Text:             stringValue(request.Text),
		MentionData:      regularMentionData(mentions),
	}
	if slot.directory.isRequest(request.Chat) {
		task.Source = table.MESSENGER_INBOX_PENDING_REQUESTS
	}
	if media != nil {
		upload, err := client.GetHTTP().SendMercuryUploadRequest(ctx, threadID, &httpclient.MercuryUploadMedia{
			Filename: firstNonEmpty(media.filename, defaultFilename(mediaKindFile, media.contentType)), MimeType: media.contentType, MediaData: media.data,
		})
		if err != nil {
			return nil, wrapMetaError(err)
		}
		attachmentID := upload.Payload.RealMetadata.GetFbId()
		if attachmentID == 0 {
			return nil, errMetaRejected
		}
		task.SendType = table.MEDIA
		task.AttachmentFBIds = []int64{attachmentID}
	}
	if request.ReplyTo != nil && *request.ReplyTo != "" {
		parsed, err := parseSourceID(*request.ReplyTo)
		if err != nil || parsed.encrypted {
			return nil, errBadRequest
		}
		replySender := int64(0)
		if ref, known := slot.messages.lookup(*request.ReplyTo); known {
			replySender = ref.senderID
		}
		task.ReplyMetaData = &socket.ReplyMetaData{ReplyMessageId: parsed.id, ReplySourceType: 1, ReplySender: replySender}
	}
	readTask := &socket.ThreadMarkReadTask{ThreadId: threadID, LastReadWatermarkTs: time.Now().UnixMilli(), SyncGroup: defaultSyncGroup}

	slot.sentIDs.add(otidText)
	startedAt := time.Now()
	var response *table.LSTable
	for attempt := 1; attempt <= maxRegularSendAttempts; attempt++ {
		response, err = client.ExecuteTasks(ctx, task, readTask)
		if err == nil || ctx.Err() != nil {
			break
		}
		slot.log.Warn().Str("event", "regular_send_retry").Int("attempt", attempt).Err(err).Send()
	}
	if err != nil {
		return nil, wrapMetaError(err)
	}
	messageID, err := optimisticMessageID(response, otidText)
	if err != nil {
		return nil, err
	}
	slot.sentIDs.add(regularSourceID(messageID))
	return &sendResult{
		sourceID: regularSourceID(messageID),
		timings:  map[string]int64{"devices": 0, "encrypt": 0, "ack": time.Since(startedAt).Milliseconds()},
	}, nil
}

func optimisticMessageID(response *table.LSTable, otid string) (string, error) {
	if response == nil {
		return "", errMetaRejected
	}
	for _, replaced := range response.LSReplaceOptimsiticMessage {
		if replaced.OfflineThreadingId == otid && replaced.MessageId != "" {
			return replaced.MessageId, nil
		}
	}
	return "", errMetaRejected
}

// ---------- actions ----------

func (slot *Slot) React(ctx context.Context, chat Chat, targetSourceID, emoji string) error {
	target, err := parseSourceID(targetSourceID)
	if err != nil {
		return err
	}
	slot.sentIDs.add(reactionEchoKey(targetSourceID, emoji))
	if target.encrypted {
		return slot.sendEncryptedAction(ctx, chat, wrapConsumerContent(&waConsumerApplication.ConsumerApplication_Content_ReactionMessage{
			ReactionMessage: &waConsumerApplication.ConsumerApplication_ReactionMessage{
				Key:               slot.encryptedTargetKey(chat, target, targetSourceID, false),
				Text:              proto.String(emoji),
				SenderTimestampMS: proto.Int64(time.Now().UnixMilli()),
			},
		}))
	}
	client, err := slot.readyRegularClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.ExecuteTasks(ctx, &socket.SendReactionTask{
		ThreadKey:       chat.numericID(),
		TimestampMs:     time.Now().UnixMilli(),
		MessageID:       target.id,
		Reaction:        emoji,
		ActorID:         slot.selfID(),
		SyncGroup:       defaultSyncGroup,
		SendAttribution: table.MESSENGER_INBOX_IN_THREAD,
	})
	return wrapMetaError(err)
}

func (slot *Slot) Unsend(ctx context.Context, chat Chat, targetSourceID string) error {
	target, err := parseSourceID(targetSourceID)
	if err != nil {
		return err
	}
	slot.reportedEvents.add("unsend:" + targetSourceID)
	if target.encrypted {
		return slot.sendEncryptedAction(ctx, chat, &waConsumerApplication.ConsumerApplication{
			Payload: &waConsumerApplication.ConsumerApplication_Payload{
				Payload: &waConsumerApplication.ConsumerApplication_Payload_ApplicationData{
					ApplicationData: &waConsumerApplication.ConsumerApplication_ApplicationData{
						ApplicationContent: &waConsumerApplication.ConsumerApplication_ApplicationData_Revoke{
							Revoke: &waConsumerApplication.ConsumerApplication_RevokeMessage{
								Key: slot.encryptedTargetKey(chat, target, targetSourceID, true),
							},
						},
					},
				},
			},
		})
	}
	client, err := slot.readyRegularClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.ExecuteTasks(ctx, &socket.DeleteMessageTask{MessageId: target.id})
	return wrapMetaError(err)
}

func (slot *Slot) SetTyping(ctx context.Context, chat Chat, typing bool) error {
	route, err := slot.routeFor(ctx, chat)
	if err != nil {
		return err
	}
	if route == routeEncrypted {
		client := slot.encryptedClient()
		if client == nil {
			return errNotConnected
		}
		slot.ensurePresenceAvailable(ctx, client)
		state := waTypes.ChatPresencePaused
		if typing {
			state = waTypes.ChatPresenceComposing
		}
		return wrapMetaError(client.SendChatPresence(ctx, encryptedChatJID(chat), state, waTypes.ChatPresenceMediaText))
	}
	client := slot.regularClient()
	if client == nil {
		return errNotConnected
	}
	isGroup, isTyping := int64(1), int64(0)
	if chat.Kind == chatKindDirect {
		isGroup = 0
	}
	if typing {
		isTyping = 1
	}
	return wrapMetaError(client.ExecuteStatelessTask(ctx, &socket.UpdatePresenceTask{
		ThreadKey:     chat.numericID(),
		IsGroupThread: isGroup,
		IsTyping:      isTyping,
		SyncGroup:     defaultSyncGroup,
		ThreadType:    int64(slot.directory.threadType(chat)),
	}))
}

// ensurePresenceAvailable marks the account online once on the encrypted
// transport; Messenger only relays typing from available clients.
func (slot *Slot) ensurePresenceAvailable(ctx context.Context, client *whatsmeow.Client) {
	slot.mu.Lock()
	alreadyAvailable := slot.presenceAvailable
	slot.presenceAvailable = true
	slot.mu.Unlock()
	if alreadyAvailable {
		return
	}
	if err := client.SendPresence(ctx, waTypes.PresenceAvailable); err != nil {
		slot.mu.Lock()
		slot.presenceAvailable = false
		slot.mu.Unlock()
	}
}

func (slot *Slot) MarkRead(ctx context.Context, chat Chat) error {
	route, err := slot.routeFor(ctx, chat)
	if err != nil && !errors.Is(err, errNotConnected) {
		return err
	}
	if route == routeEncrypted {
		client := slot.encryptedClient()
		if client == nil {
			return errNotConnected
		}
		reads := slot.messages.takePendingReads(chat)
		bySender := make(map[int64][]waTypes.MessageID)
		for _, read := range reads {
			bySender[read.senderID] = append(bySender[read.senderID], read.stanzaID)
		}
		for senderID, ids := range bySender {
			sender := waTypes.EmptyJID
			if chat.Kind == chatKindGroup {
				sender = waTypes.NewJID(formatFBID(senderID), waTypes.MessengerServer)
			}
			if err := client.MarkRead(ctx, ids, time.Now(), encryptedChatJID(chat), sender); err != nil {
				return wrapMetaError(err)
			}
		}
		return nil
	}
	client, err := slot.readyRegularClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.ExecuteTasks(ctx, &socket.ThreadMarkReadTask{
		ThreadId: chat.numericID(), LastReadWatermarkTs: time.Now().UnixMilli(), SyncGroup: defaultSyncGroup,
	})
	return wrapMetaError(err)
}

func (slot *Slot) AcceptRequest(ctx context.Context, chat Chat) error {
	client, err := slot.readyRegularClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.ExecuteTasks(ctx, &socket.AcceptMessageRequestTask{
		ThreadKey: slot.directory.requestThreadKey(chat),
		SyncGroup: messageRequestSyncGrp,
	})
	if err != nil {
		return wrapMetaError(err)
	}
	slot.directory.markAccepted(chat)
	return nil
}

// wrapMetaError keeps contract errors as they are and reports anything else
// from the libraries as Meta rejecting the request.
func wrapMetaError(err error) error {
	if err == nil {
		return nil
	}
	var typed *apiError
	if errors.As(err, &typed) {
		return typed
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return newAPIError(http.StatusGatewayTimeout, codeUnknown, "Messenger did not answer in time.")
	}
	if strings.Contains(err.Error(), "not connected") || errors.Is(err, whatsmeow.ErrNotConnected) {
		return errNotConnected
	}
	return fmt.Errorf("%w (%s)", errMetaRejected, err.Error())
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
