package client

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/sealjay/mcp-whatsapp/internal/security"
	"github.com/sealjay/mcp-whatsapp/internal/store"
)

// StartEventHandler registers the event handler on the underlying whatsmeow
// client. Must be called before Connect().
func (c *Client) StartEventHandler() {
	if c.handlerInstalled {
		return
	}
	c.handlerID = c.wa.AddEventHandler(func(evt interface{}) {
		switch evt.(type) {
		case *events.Message, *events.DeleteForMe, *events.MarkChatAsRead, *events.Receipt:
			finish := c.syncState.processing()
			defer finish(nil)
		}

		switch v := evt.(type) {
		case *events.Message:
			c.handleMessage(v)
		case *events.DeleteForMe:
			if err := c.store.DeleteForMe(context.Background(), c.store.ResolveLIDToJID(v.ChatJID.String()), v.MessageID); err != nil {
				c.syncState.recordFailure(err)
				c.log.Warnf("Delete-for-me storage failed: %v", err)
			}
		case *events.HistorySync:
			c.handleHistorySync(v)
		case *events.AppStateSyncError:
			c.syncState.collectionFailure(string(v.Name), v.Error)
		case *events.AppStateSyncComplete:
			c.syncState.collectionComplete(string(v.Name))
		case *events.Disconnected:
			c.syncState.connection(false)
		case *events.Connected:
			c.syncState.connection(false)
			c.log.Infof("Connected to WhatsApp")
			if c.wa.Store.ID != nil {
				if err := c.store.SetOwnIdentity(c.wa.Store.ID.ToNonAD().String(), c.wa.Store.LID.ToNonAD().String()); err != nil {
					c.log.Warnf("Own identity storage failed: %v", err)
				}
			}
			finish := c.syncState.processing()
			go func() { defer finish(nil); c.refreshReadState() }()
		case *events.LoggedOut:
			c.syncState.connection(false)
			c.log.Warnf("Device logged out, please scan QR code to log in again")
		case *events.OfflineSyncPreview:
			c.syncState.connection(false)
			c.log.Infof("Offline sync starting - %d messages, %d receipts pending", v.Messages, v.Receipts)
		case *events.OfflineSyncCompleted:
			c.syncState.connection(true)
			c.log.Infof("Offline sync completed - %d messages synced", v.Count)
		case *events.MarkChatAsRead:
			if v.Action != nil && v.Action.Read != nil {
				chat := c.store.ResolveLIDToJID(v.JID.String())
				ts := v.Action.GetMessageRange().GetLastMessageTimestamp()
				through := time.Time{}
				if ts > 0 {
					through = time.Unix(ts, 0)
				}
				if err := c.store.ObserveChatRead(context.Background(), chat, v.Action.GetRead(), through, v.Timestamp); err != nil {
					c.syncState.recordFailure(err)
					c.log.Warnf("Read-state storage failed: %v", err)
				}
			}
		case *events.Receipt:
			if v.Type == types.ReceiptTypeReadSelf {
				if err := c.store.ObserveReadIDs(context.Background(), c.store.ResolveLIDToJID(v.Chat.String()), v.MessageIDs); err != nil {
					c.syncState.recordFailure(err)
					c.log.Warnf("Read receipt storage failed: %v", err)
				}
			}
			c.log.Debugf("Receipt for %d messages: %s", len(v.MessageIDs), v.Type)
		case *events.UndecryptableMessage:
			c.log.Warnf("Undecryptable message from %s", v.Info.Sender)
		default:
			c.log.Debugf("Unhandled event type: %T", v)
		}
	})
	c.handlerInstalled = true
}

// normalizedMessage holds the result of normalizeIncomingMessage: the
// store-ready Message plus the raw media crypto fields and the full
// post-LID-resolution sender JID (used for poll-vote keying).
type normalizedMessage struct {
	msg           store.Message
	mediaKey      []byte
	fileSHA256    []byte
	fileEncSHA256 []byte
	fileLength    uint64
	senderFullJID string // post-LID sender as "user@server"
	chatJID       string // post-LID-normalised chat JID
}

// normalizeIncomingMessage resolves sender and chat JIDs (LID → phone),
// extracts text/media content, and returns a store-ready normalizedMessage.
// It covers the logic shared between handleMessage and handleHistorySync.
//
// Parameters:
//   - rawChatJID: the raw chat JID string (may be @lid for DMs).
//   - rawSenderJID: the raw sender JID string (may be @lid).
//   - senderUser: the user-part of the sender JID (fallback when no
//     participant is available).
//   - isGroup: true when rawChatJID ends with @g.us.
//   - isFromMe: whether the message was sent by the local device.
//   - ownID: the local device's user-part (used when isFromMe is true and
//     no participant is set).
//   - msgID: the message ID.
//   - timestamp: the message timestamp.
//   - raw: the protobuf Message payload.
func (c *Client) normalizeIncomingMessage(
	rawChatJID, rawSenderJID, senderUser string,
	isGroup, isFromMe bool,
	ownID string,
	msgID string,
	timestamp time.Time,
	raw *waProto.Message,
) normalizedMessage {
	// Normalise chat JID: convert LID to standard JID for direct chats.
	chatJID := rawChatJID
	if !isGroup {
		chatJID = c.store.ResolveLIDToJID(rawChatJID)
		if chatJID != rawChatJID {
			c.log.Infof("Normalized chat JID: %s -> %s", c.redactor.JID(rawChatJID), c.redactor.JID(chatJID))
		}
	}

	// Preserve the identity namespace. A LID is not a phone number.
	normalizedSender := c.store.ResolveLIDToJID(rawSenderJID)
	sender := normalizedSender
	if isFromMe && sender == "" && ownID != "" {
		sender = ownID + "@s.whatsapp.net"
	}

	content := extractTextContent(raw)
	if poll := pollCreation(raw); poll != nil {
		content = poll.GetName()
	}
	mediaType, filename, url, directPath, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(raw)
	if pollCreation(raw) != nil {
		mediaType = "poll"
	}

	return normalizedMessage{
		msg: store.Message{
			MentionJIDs: extractMentionJIDs(raw),
			ID:          msgID,
			ChatJID:     chatJID,
			Sender:      sender,
			Content:     content,
			Timestamp:   timestamp,
			IsFromMe:    isFromMe,
			MediaType:   mediaType,
			Filename:    filename,
			URL:         url,
			DirectPath:  directPath,
		},
		mediaKey:      mediaKey,
		fileSHA256:    fileSHA256,
		fileEncSHA256: fileEncSHA256,
		fileLength:    fileLength,
		senderFullJID: normalizedSender,
		chatJID:       chatJID,
	}
}

// handleMessage persists an incoming message and its chat metadata.
func (c *Client) handleMessage(msg *events.Message) {
	if event, mutation, err := c.protocolMutation(msg.Info.Chat.String(), msg.Message); mutation {
		if err == nil {
			_, err = c.store.ApplyMessages(context.Background(), []store.MessageEvent{event})
		}
		if err != nil {
			c.syncState.recordFailure(err)
			c.log.Warnf("Message mutation failed: %v", err)
		}
		return
	}
	nm := c.normalizeIncomingMessage(
		msg.Info.Chat.String(),
		msg.Info.Sender.String(),
		msg.Info.Sender.User,
		strings.HasSuffix(msg.Info.Chat.String(), "@g.us"),
		msg.Info.IsFromMe,
		"", // ownID not needed for live messages — sender is always set
		msg.Info.ID,
		msg.Info.Timestamp,
		msg.Message,
	)

	name := c.GetChatName(msg.Info.Chat, nm.chatJID, nil, nm.msg.Sender)

	// Poll vote — a signal-only event with no user-visible content. Decrypt,
	// tally into the local cache, and return. We pass the post-LID-resolution
	// full JID so StorePollVote's primary key never collides across servers.
	if msg.Message != nil && msg.Message.GetPollUpdateMessage() != nil {
		voterFullJID := nm.senderFullJID
		if voterFullJID == "" {
			voterFullJID = msg.Info.Sender.String()
		}
		c.handlePollVote(msg, nm.chatJID, voterFullJID)
		return
	}

	// Poll creation — store a "[poll] <question>" row for readability and
	// cache the option names so we can decode vote SHA-256 hashes later.
	var pollOptions []string
	if msg.Message != nil {
		if pc := pollCreation(msg.Message); pc != nil {
			pollOptions = extractPollOptionNames(pc)
			if nm.msg.Content == "" {
				nm.msg.Content = pc.GetName()
				nm.msg.MediaType = "poll"
			}
		}
	}

	if nm.msg.Content == "" && nm.msg.MediaType == "" {
		return
	}

	_, err := c.store.ApplyMessages(context.Background(), []store.MessageEvent{{Message: nm.msg, ChatName: name, LegacySender: msg.Info.Sender.User, ContentUnknown: !contentKnown(msg.Message), Incoming: true, PollOptions: pollOptions, MediaKey: nm.mediaKey, FileSHA256: nm.fileSHA256, FileEncSHA256: nm.fileEncSHA256, FileLength: nm.fileLength}})
	if err != nil {
		c.syncState.recordFailure(err)
		c.log.Warnf("Failed to apply message event: %v", err)
		return
	}
	direction := "<-"
	if msg.Info.IsFromMe {
		direction = "->"
	}
	ts := msg.Info.Timestamp.Format("2006-01-02 15:04:05")
	if nm.msg.MediaType != "" {
		c.log.Infof("[%s] %s %s: [%s: %s] %s", ts, direction, c.redactor.JID(nm.msg.Sender), nm.msg.MediaType, nm.msg.Filename, c.redactor.Body(nm.msg.Content))
	} else if nm.msg.Content != "" {
		c.log.Infof("[%s] %s %s: %s", ts, direction, c.redactor.JID(nm.msg.Sender), c.redactor.Body(nm.msg.Content))
	}
}

// handleHistorySync processes a bulk history payload from WhatsApp.
func (c *Client) handleHistorySync(historySync *events.HistorySync) {
	finish := c.syncState.processing()
	var importErr error
	defer func() { c.completeHistory(historySync, importErr); finish(importErr) }()
	if historySync == nil || historySync.Data == nil {
		importErr = fmt.Errorf("invalid history payload")
		return
	}

	syncType := "UNKNOWN"
	if historySync.Data.SyncType != nil {
		syncType = historySync.Data.SyncType.String()
	}
	c.log.Infof("Received history sync event (type: %s) with %d conversations",
		syncType, len(historySync.Data.Conversations))

	ctx := context.Background()
	var projected []store.MessageEvent
	var chatSnapshots []store.ChatSnapshot

	ownID := ""
	if c.wa.Store.ID != nil {
		ownID = c.wa.Store.ID.User
	}

	for _, conversation := range historySync.Data.Conversations {
		if conversation.ID == nil {
			continue
		}
		rawChatJID := *conversation.ID

		jid, err := types.ParseJID(rawChatJID)
		if err != nil {
			c.log.Warnf("Failed to parse JID %s: %v", rawChatJID, err)
			continue
		}

		isGroup := strings.HasSuffix(rawChatJID, "@g.us")

		// Resolve the chat JID once for the conversation header. We
		// normalise it here so GetChatName and StoreChat use the
		// phone-number-based JID.
		chatJID := rawChatJID
		if !isGroup {
			chatJID = c.store.ResolveLIDToJID(rawChatJID)
			if chatJID != rawChatJID {
				c.log.Infof("History sync: Normalized chat JID: %s -> %s", c.redactor.JID(rawChatJID), c.redactor.JID(chatJID))
			}
		}

		name := c.GetChatName(jid, chatJID, conversation, "")

		chatSnapshots = append(chatSnapshots, store.ChatSnapshot{JID: chatJID, Name: name, LastMessageTime: time.Unix(int64(conversation.GetLastMsgTimestamp()), 0), UnreadCount: conversation.UnreadCount, MarkedUnread: conversation.MarkedAsUnread})
		messages := conversation.Messages
		if len(messages) == 0 {
			continue
		}

		for _, msg := range messages {
			if msg == nil || msg.Message == nil {
				continue
			}

			raw := unwrapHistoryMessage(msg.Message.Message)
			if event, mutation, err := c.protocolMutation(rawChatJID, raw); mutation {
				if err != nil {
					importErr = err
					return
				}
				projected = append(projected, event)
				continue
			}
			msgTS := msg.Message.GetMessageTimestamp()
			if msgTS == 0 {
				continue
			}
			timestamp := time.Unix(int64(msgTS), 0)

			// History can carry its participant on WebMessageInfo rather than the
			// message key. A missing participant never identifies the group itself.
			isFromMe := msg.Message.GetKey().GetFromMe()
			rawSenderJID, senderUser := historySender(rawChatJID, ownID, isFromMe,
				msg.Message.GetKey().GetParticipant(), msg.Message.GetParticipant())

			msgID := ""
			if msg.Message.Key != nil && msg.Message.Key.ID != nil {
				msgID = *msg.Message.Key.ID
			}

			nm := c.normalizeIncomingMessage(
				rawChatJID, rawSenderJID, senderUser,
				isGroup, isFromMe, ownID,
				msgID, timestamp, raw,
			)

			c.log.Infof("Message content: %s, Media Type: %s", c.redactor.Body(nm.msg.Content), nm.msg.MediaType)
			if nm.msg.Content == "" && nm.msg.MediaType == "" {
				continue
			}

			// Override chatJID to the already-resolved one from the
			// conversation header — avoids re-resolving per message.
			nm.msg.ChatJID = chatJID

			projected = append(projected, store.MessageEvent{Message: nm.msg, ChatName: name, LegacySender: senderUser, ContentUnknown: !contentKnown(raw), PollOptions: extractPollOptionNames(pollCreation(raw)), MediaKey: nm.mediaKey, FileSHA256: nm.fileSHA256, FileEncSHA256: nm.fileEncSHA256, FileLength: nm.fileLength})

		}
	}

	result, err := c.store.ApplyHistory(ctx, projected, chatSnapshots)
	if err != nil {
		importErr = err
		c.log.Warnf("History transaction failed: %v", err)
		return
	}
	c.log.Infof("History sync committed: %d inserted, %d suppressed, watermark %d", result.Inserted, result.Suppressed, result.Watermark)
}

// GetChatName determines the appropriate name for a chat. It checks the
// existing database entry first, then falls back to conversation metadata
// (for history sync), contact store lookups, and finally the JID.
// The conversation parameter is the typed whatsmeow Conversation from a
// history-sync payload; pass nil for live messages.
func (c *Client) GetChatName(jid types.JID, chatJID string, conversation *waHistorySync.Conversation, sender string) string {
	// A conversation is identified by its peer, not the author of whichever
	// message happened to create the row (which can be our own account).
	if !strings.HasSuffix(chatJID, "@g.us") {
		if name := c.store.ContactName(chatJID); name != "" {
			return name
		}
	}
	if existing := c.store.FindChatName(chatJID); existing != "" {
		c.log.Infof("Using existing chat name for %s: %s", chatJID, existing)
		return existing
	}

	var name string

	if jid.Server == "g.us" {
		c.log.Infof("Getting name for group: %s", chatJID)

		if conversation != nil {
			if dn := conversation.GetDisplayName(); dn != "" {
				name = dn
			} else if cn := conversation.GetName(); cn != "" {
				name = cn
			}
		}

		if name == "" {
			name = c.resolveChatNameFallback(context.Background(), jid, sender)
		}

		c.log.Infof("Using group name: %s", name)
	} else {
		c.log.Infof("Getting name for contact: %s", chatJID)

		name = c.resolveChatNameFallback(context.Background(), jid, sender)

		c.log.Infof("Using contact name: %s", name)
	}

	return name
}

// resolveChatNameFallback looks up a chat's name via GetGroupInfo (for
// groups) or the contact store (for DMs), falling back to sender (if given)
// and finally jid.User. This is the shared tail of GetChatName's group and
// contact branches, also used by outgoing projection (which has no
// conversation metadata to check first).
func (c *Client) resolveChatNameFallback(ctx context.Context, jid types.JID, sender string) string {
	if jid.Server == "g.us" {
		if gi, err := c.wa.GetGroupInfo(ctx, jid); err == nil && gi.Name != "" {
			return gi.Name
		}
		return "Group " + jid.User
	}
	if contact, err := c.wa.Store.Contacts.GetContact(ctx, jid); err == nil && contact.FullName != "" {
		return contact.FullName
	}
	// An outgoing sender is us, not the peer. Never use it as the chat name.
	return jid.User
}

// extractTextContent returns the plain-text body of a Message, if any.
func extractTextContent(msg *waProto.Message) string {
	if msg == nil {
		return ""
	}
	if text := msg.GetConversation(); text != "" {
		return text
	}
	if ext := msg.GetExtendedTextMessage(); ext != nil {
		return ext.GetText()
	}
	if image := msg.GetImageMessage(); image != nil {
		return image.GetCaption()
	}
	if video := msg.GetVideoMessage(); video != nil {
		return video.GetCaption()
	}
	if doc := msg.GetDocumentMessage(); doc != nil {
		return doc.GetCaption()
	}
	return ""
}

// extractMediaInfo pulls out the storable media fields from a Message.
// directPath is captured verbatim from the media protobuf and includes the
// signed `?ccb=&oh=&oe=&_nc_sid=` CDN auth query, so the download path can
// hand it straight to whatsmeow without any URL string manipulation.
func extractMediaInfo(msg *waProto.Message) (mediaType, filename, url, directPath string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) {
	if msg == nil {
		return "", "", "", "", nil, nil, nil, 0
	}

	if img := msg.GetImageMessage(); img != nil {
		return "image", generatedMediaFilename("image", img.GetFileSHA256(), ".jpg"),
			img.GetURL(), img.GetDirectPath(),
			img.GetMediaKey(), img.GetFileSHA256(), img.GetFileEncSHA256(), img.GetFileLength()
	}
	if vid := msg.GetVideoMessage(); vid != nil {
		return "video", generatedMediaFilename("video", vid.GetFileSHA256(), ".mp4"),
			vid.GetURL(), vid.GetDirectPath(),
			vid.GetMediaKey(), vid.GetFileSHA256(), vid.GetFileEncSHA256(), vid.GetFileLength()
	}
	if aud := msg.GetAudioMessage(); aud != nil {
		return "audio", generatedMediaFilename("audio", aud.GetFileSHA256(), ".ogg"),
			aud.GetURL(), aud.GetDirectPath(),
			aud.GetMediaKey(), aud.GetFileSHA256(), aud.GetFileEncSHA256(), aud.GetFileLength()
	}
	if doc := msg.GetDocumentMessage(); doc != nil {
		fname := security.SafeFilename(doc.GetFileName())
		return "document", fname,
			doc.GetURL(), doc.GetDirectPath(),
			doc.GetMediaKey(), doc.GetFileSHA256(), doc.GetFileEncSHA256(), doc.GetFileLength()
	}

	return "", "", "", "", nil, nil, nil, 0
}

func generatedMediaFilename(prefix string, fileSHA256 []byte, extension string) string {
	digest := hex.EncodeToString(fileSHA256)
	if digest == "" {
		digest = "unknown"
	}
	return prefix + "_" + digest + extension
}

// extractPollOptionNames pulls the option names out of a PollCreationMessage.
// Returns nil if the input is nil or the options list is empty.
func extractPollOptionNames(pc *waProto.PollCreationMessage) []string {
	if pc == nil {
		return nil
	}
	opts := pc.GetOptions()
	if len(opts) == 0 {
		return nil
	}
	out := make([]string, 0, len(opts))
	for _, o := range opts {
		out = append(out, o.GetOptionName())
	}
	return out
}

// handlePollVote decrypts an incoming PollUpdateMessage, reverses its
// SelectedOptions SHA-256 hashes against the cached option names, and records
// the voter's current selection. Failures are logged (without poll content)
// and swallowed so a broken vote never blocks the rest of message handling.
// voterJID must be the full post-LID-resolution JID string.
func (c *Client) handlePollVote(msg *events.Message, chatJID, voterJID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	vote, err := c.wa.DecryptPollVote(ctx, msg)
	if err != nil {
		c.log.Warnf("Failed to decrypt poll vote from %s in %s: %v", voterJID, chatJID, err)
		return
	}

	pu := msg.Message.GetPollUpdateMessage()
	key := pu.GetPollCreationMessageKey()
	pollID := key.GetID()
	if pollID == "" {
		c.log.Warnf("Poll vote missing poll creation message id (chat=%s)", chatJID)
		return
	}

	// Scope lookup to our locally-observed chatJID; ignore key.RemoteJID to
	// prevent a crafted update from another chat polluting this chat's tally.
	options, err := c.store.GetPollOptions(ctx, pollID, chatJID)
	if err != nil {
		// No cached metadata — we've probably never seen the poll creation
		// message. Log without listing option text and bail.
		c.log.Warnf("Poll vote for unknown poll %s in %s: %v", pollID, chatJID, err)
		return
	}

	optionHashes := whatsmeow.HashPollOptions(options)
	selectedHashes := vote.GetSelectedOptions()
	selected := make([]string, 0, len(selectedHashes))
	for _, h := range selectedHashes {
		for i, known := range optionHashes {
			if bytes.Equal(h, known) {
				selected = append(selected, options[i])
				break
			}
		}
	}

	// Use the server envelope's event time. Never rewrite it using the local
	// wall clock or trust a peer-supplied future time as a newer revision.
	ts := msg.Info.Timestamp

	if err := c.store.StorePollVote(ctx, pollID, chatJID, voterJID, selected, ts); err != nil {
		c.log.Warnf("Failed to store poll vote for poll %s in %s: %v", pollID, chatJID, err)
		return
	}

	c.log.Debugf("Recorded poll vote from %s on poll %s with %d selections", voterJID, pollID, len(selected))
}

// historySender resolves the two participant locations used by history sync.
// Unknown group senders remain unknown instead of overwriting a known sender
// with the group's identity.
func historySender(chatJID, ownID string, fromMe bool, keyParticipant, infoParticipant string) (string, string) {
	if fromMe {
		if ownID == "" {
			return "", ""
		}
		return ownID + "@s.whatsapp.net", ownID
	}
	participant := keyParticipant
	if participant == "" || participant == chatJID {
		participant = infoParticipant
	}
	if participant != "" && participant != chatJID {
		return participant, strings.Split(participant, "@")[0]
	}
	if strings.HasSuffix(chatJID, "@g.us") {
		return "", ""
	}
	return chatJID, strings.Split(chatJID, "@")[0]
}

// Read actual mention metadata, never infer mentions from arbitrary @numbers.
func extractMentionJIDs(msg *waProto.Message) []string {
	if msg == nil {
		return nil
	}
	if x := msg.GetExtendedTextMessage(); x != nil {
		return x.GetContextInfo().GetMentionedJID()
	}
	if x := msg.GetImageMessage(); x != nil {
		return x.GetContextInfo().GetMentionedJID()
	}
	if x := msg.GetVideoMessage(); x != nil {
		return x.GetContextInfo().GetMentionedJID()
	}
	if x := msg.GetDocumentMessage(); x != nil {
		return x.GetContextInfo().GetMentionedJID()
	}
	if x := msg.GetAudioMessage(); x != nil {
		return x.GetContextInfo().GetMentionedJID()
	}
	if x := msg.GetStickerMessage(); x != nil {
		return x.GetContextInfo().GetMentionedJID()
	}
	return nil
}
