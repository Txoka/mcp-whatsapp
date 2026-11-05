package client

import (
	"fmt"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types/events"
)

// protocolMutation decodes mutations of the original message key, never the
// transport envelope ID. Missing revisions are errors, not guessed timestamps.
func (c *Client) protocolMutation(chat string, raw *waProto.Message) (store.MessageEvent, bool, error) {
	p := raw.GetProtocolMessage()
	if p == nil {
		return store.MessageEvent{}, false, nil
	}
	kind := ""
	switch p.GetType() {
	case waProto.ProtocolMessage_REVOKE:
		kind = "revoke"
	case waProto.ProtocolMessage_MESSAGE_EDIT:
		kind = "edit"
	default:
		return store.MessageEvent{}, false, nil
	}
	key := p.GetKey()
	if key.GetID() == "" {
		return store.MessageEvent{}, true, fmt.Errorf("mutation has no original message key")
	}
	chat = c.store.ResolveLIDToJID(chat)
	if target := key.GetRemoteJID(); target != "" && c.store.ResolveLIDToJID(target) != chat {
		return store.MessageEvent{}, true, fmt.Errorf("mutation chat does not match envelope")
	}
	m := store.Message{ID: key.GetID(), ChatJID: chat}
	if kind == "edit" {
		m.Content = extractTextContent(p.GetEditedMessage())
		m.MentionJIDs = extractMentionJIDs(p.GetEditedMessage())
	}
	return store.MessageEvent{Kind: kind, Message: m, RevisionMS: p.GetTimestampMS()}, true, nil
}
func unwrapHistoryMessage(raw *waProto.Message) *waProto.Message {
	return (&events.Message{RawMessage: raw}).UnwrapRaw().Message
}

func contentKnown(raw *waProto.Message) bool {
	if raw == nil {
		return false
	}
	if raw.Conversation != nil {
		return true
	}
	if x := raw.GetExtendedTextMessage(); x != nil {
		return x.Text != nil
	}
	if x := raw.GetImageMessage(); x != nil {
		return true
	}
	if x := raw.GetVideoMessage(); x != nil {
		return true
	}
	if x := raw.GetDocumentMessage(); x != nil {
		return true
	}
	return pollCreation(raw) != nil
}

func pollCreation(raw *waProto.Message) *waProto.PollCreationMessage {
	if raw == nil {
		return nil
	}
	if p := raw.GetPollCreationMessage(); p != nil {
		return p
	}
	if p := raw.GetPollCreationMessageV2(); p != nil {
		return p
	}
	return raw.GetPollCreationMessageV3()
}
