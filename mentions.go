package main

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"

	"go.mau.fi/whatsmeow/proto/waCommon"
	waTypes "go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// mention marks `@name` inside a message text. Offset and length count UTF-16
// code units, the unit Messenger itself uses for mentions.
type mention struct {
	ID     string `json:"id"`
	Name   string `json:"name,omitempty"`
	Offset int    `json:"offset"`
	Length int    `json:"length"`
}

// mentionEveryoneID is the contract's id for a mention of the whole group.
const mentionEveryoneID = "all"

func utf16Length(text string) int {
	return len(utf16.Encode([]rune(text)))
}

// regularMentions reads the person and whole-group mentions Meta attached to
// a regular message; the name is the mentioned text itself, as shown to the
// reader.
func regularMentions(text string, message *table.LSInsertMessage) []mention {
	parsed, err := (&socket.MentionData{
		MentionIDs: message.MentionIds, MentionOffsets: message.MentionOffsets,
		MentionLengths: message.MentionLengths, MentionTypes: message.MentionTypes,
	}).Parse()
	if err != nil || len(parsed) == 0 {
		return nil
	}
	units := utf16.Encode([]rune(text))
	var mentions []mention
	for _, item := range parsed {
		id := formatFBID(item.ID)
		switch item.Type {
		case socket.MentionTypePerson, socket.MentionTypeSilent:
		case socket.MentionTypeThread:
			id = mentionEveryoneID
		default:
			continue
		}
		end := item.Offset + item.Length
		if id == "" || item.Offset < 0 || item.Length <= 0 || end > len(units) {
			continue
		}
		name := strings.TrimPrefix(string(utf16.Decode(units[item.Offset:end])), "@")
		if name == "" {
			continue
		}
		mentions = append(mentions, mention{ID: id, Name: name, Offset: item.Offset, Length: item.Length})
	}
	return mentions
}

// absorbEncryptedText sets the text of an encrypted message. Messenger writes
// a mention into the text as `@<jid>`; it becomes `@name` so the text reads
// like the app, with the span reported as a mention. A whole-group mention
// is an EVERYONE command over its span of the original text.
func (slot *Slot) absorbEncryptedText(content *messageContent, text *waCommon.MessageText) {
	body := text.GetText()
	jids := text.GetMentionedJID()
	everyone := everyoneSpans(body, text.GetCommands())
	if (len(jids) == 0 && len(everyone) == 0) || body == "" {
		content.Text = optionalString(body)
		return
	}
	ids := make([]int64, 0, len(jids))
	for _, raw := range jids {
		if parsed, err := waTypes.ParseJID(raw); err == nil {
			ids = append(ids, jidFBID(parsed))
		}
	}
	ctx, cancel := context.WithTimeout(slot.ctx, profileWait)
	slot.resolveContacts(ctx, ids)
	cancel()

	type occurrence struct {
		start, end int
		id         string
		name       string
	}
	var found []occurrence
	overlaps := func(start, end int) bool {
		for _, existing := range found {
			if start < existing.end && existing.start < end {
				return true
			}
		}
		return false
	}
	for _, raw := range jids {
		parsed, err := waTypes.ParseJID(raw)
		id := jidFBID(parsed)
		name := slot.directory.contact(id).name
		if err != nil || id == 0 || name == "" {
			continue
		}
		for _, token := range []string{"@" + raw, "@" + parsed.User} {
			for from := 0; from < len(body); {
				index := strings.Index(body[from:], token)
				if index < 0 {
					break
				}
				start := from + index
				if !overlaps(start, start+len(token)) {
					found = append(found, occurrence{start: start, end: start + len(token), id: formatFBID(id), name: name})
				}
				from = start + len(token)
			}
		}
	}
	for _, span := range everyone {
		if !overlaps(span.start, span.end) {
			found = append(found, occurrence{start: span.start, end: span.end, id: mentionEveryoneID, name: span.name})
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].start < found[j].start })
	var rewritten strings.Builder
	previous := 0
	for _, item := range found {
		rewritten.WriteString(body[previous:item.start])
		label := "@" + item.name
		content.Mentions = append(content.Mentions, mention{
			ID: item.id, Name: item.name, Offset: utf16Length(rewritten.String()), Length: utf16Length(label),
		})
		rewritten.WriteString(label)
		previous = item.end
	}
	rewritten.WriteString(body[previous:])
	content.Text = optionalString(rewritten.String())
}

type everyoneSpan struct {
	start, end int
	name       string
}

// everyoneSpans finds the EVERYONE commands' spans in text, as byte ranges.
// The command counts UTF-16 code units, like the other Messenger mentions.
func everyoneSpans(text string, commands []*waCommon.Command) []everyoneSpan {
	var spans []everyoneSpan
	units := utf16.Encode([]rune(text))
	for _, command := range commands {
		offset, length := int(command.GetOffset()), int(command.GetLength())
		if command.GetCommandType() != waCommon.Command_EVERYONE || length <= 0 || offset+length > len(units) {
			continue
		}
		start := len(string(utf16.Decode(units[:offset])))
		span := string(utf16.Decode(units[offset : offset+length]))
		if name := strings.TrimPrefix(span, "@"); name != "" {
			spans = append(spans, everyoneSpan{start: start, end: start + len(span), name: name})
		}
	}
	return spans
}

// outboundMention is a validated mention of an outgoing text: a person, or
// the whole group when everyone is set.
type outboundMention struct {
	id       int64
	everyone bool
	offset   int
	length   int
}

// validateMentions checks the spans against the text: inside it, not
// overlapping, sorted by offset. Only a group can be mentioned as a whole.
func validateMentions(chat Chat, text string, raw []mention) ([]outboundMention, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	textLength := utf16Length(text)
	mentions := make([]outboundMention, 0, len(raw))
	for _, item := range raw {
		if item.Offset < 0 || item.Length <= 0 || item.Offset+item.Length > textLength {
			return nil, errBadRequest
		}
		if item.ID == mentionEveryoneID {
			if chat.Kind != chatKindGroup {
				return nil, errBadRequest
			}
			mentions = append(mentions, outboundMention{id: chat.numericID(), everyone: true, offset: item.Offset, length: item.Length})
			continue
		}
		id, err := strconv.ParseInt(item.ID, 10, 64)
		if err != nil || id <= 0 {
			return nil, errBadRequest
		}
		mentions = append(mentions, outboundMention{id: id, offset: item.Offset, length: item.Length})
	}
	sort.Slice(mentions, func(i, j int) bool { return mentions[i].offset < mentions[j].offset })
	for index := 1; index < len(mentions); index++ {
		if mentions[index].offset < mentions[index-1].offset+mentions[index-1].length {
			return nil, errBadRequest
		}
	}
	return mentions, nil
}

// regularMentionData is the regular transport's form: the text stays as is
// and the spans travel beside it.
func regularMentionData(mentions []outboundMention) *socket.MentionData {
	if len(mentions) == 0 {
		return nil
	}
	converted := make(socket.Mentions, len(mentions))
	for index, item := range mentions {
		mentionType := socket.MentionTypePerson
		if item.everyone {
			mentionType = socket.MentionTypeThread
		}
		converted[index] = socket.Mention{ID: item.id, Offset: item.offset, Length: item.length, Type: mentionType}
	}
	return converted.ToData()
}

// encryptedMentionText is the encrypted transport's form: each person span
// is replaced by `@<jid>` and the jid listed, which Messenger renders as the
// person's name; a whole-group span stays as written and travels as an
// EVERYONE command over its place in the final text.
func encryptedMentionText(text string, mentions []outboundMention) *waCommon.MessageText {
	if len(mentions) == 0 {
		return &waCommon.MessageText{Text: proto.String(text)}
	}
	units := utf16.Encode([]rune(text))
	var rewritten []uint16
	var jids []string
	var commands []*waCommon.Command
	previous := 0
	for _, item := range mentions {
		rewritten = append(rewritten, units[previous:item.offset]...)
		span := units[item.offset : item.offset+item.length]
		if item.everyone {
			commands = append(commands, &waCommon.Command{
				CommandType: waCommon.Command_EVERYONE.Enum(),
				Offset:      proto.Uint32(uint32(len(rewritten))),
				Length:      proto.Uint32(uint32(len(span))),
			})
			rewritten = append(rewritten, span...)
		} else {
			jid := waTypes.NewJID(formatFBID(item.id), waTypes.MessengerServer).String()
			jids = append(jids, jid)
			rewritten = append(rewritten, utf16.Encode([]rune("@"+jid))...)
		}
		previous = item.offset + item.length
	}
	rewritten = append(rewritten, units[previous:]...)
	return &waCommon.MessageText{Text: proto.String(string(utf16.Decode(rewritten))), MentionedJID: jids, Commands: commands}
}
