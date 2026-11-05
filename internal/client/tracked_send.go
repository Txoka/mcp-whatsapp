package client

import (
	"context"
	"fmt"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
	"time"
)

// sendTracked assigns the transport ID before submission and preserves unknown
// outcomes. It is only for user-visible writes, never peer history requests.
func (c *Client) sendTracked(ctx context.Context, to types.JID, msg *waProto.Message, projection ...store.MessageEvent) (whatsmeow.SendResponse, error) {
	id := c.wa.GenerateMessageID()
	chat := c.store.ResolveLIDToJID(to.String())
	if _, _, err := c.store.ReserveOperation(ctx, store.Operation{ID: id, Kind: "send", Chat: chat, MessageID: id}); err != nil {
		return whatsmeow.SendResponse{}, err
	}
	if len(projection) == 0 {
		projection = c.outgoingProjection(chat, id, msg)
	}
	if err := c.store.CaptureOutgoing(ctx, id, chat, projection); err != nil {
		return whatsmeow.SendResponse{}, err
	}
	claimed, err := c.store.BeginSubmission(ctx, id)
	if err != nil {
		return whatsmeow.SendResponse{}, err
	}
	if !claimed {
		return whatsmeow.SendResponse{}, fmt.Errorf("send operation already submitted")
	}
	response, err := c.wa.SendMessage(ctx, to, msg, whatsmeow.SendRequestExtra{ID: id})
	if err != nil {
		observed, e := c.store.Operation(context.Background(), id)
		if e == nil && observed.State == "applied" {
			at, _ := c.store.OutgoingTimestamp(context.Background(), id)
			return whatsmeow.SendResponse{ID: id, Timestamp: at}, nil
		}
		saveErr := c.store.SetOperation(context.Background(), id, "unknown", err.Error())
		c.store.DB().Exec("UPDATE outgoing_intents SET state='unknown' WHERE id=? AND state!='complete'", id)
		return response, fmt.Errorf("send outcome unknown (operation %s); do not automatically retry: %w (journal: %v)", id, err, saveErr)
	}
	localCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = c.store.AcceptOutgoing(localCtx, id, response.Timestamp); err != nil {
		return response, fmt.Errorf("message %s accepted; local outcome recording incomplete: %w", response.ID, err)
	}
	if err = c.store.ReplayOutgoing(localCtx, chat); err != nil {
		return response, fmt.Errorf("message %s accepted; local projection pending; do not resend: %w", response.ID, err)
	}

	return response, nil
}

func (c *Client) outgoingProjection(chat, id string, msg *waProto.Message) []store.MessageEvent {
	body := extractTextContent(msg)
	kind, name, url, direct, key, sha, enc, length := extractMediaInfo(msg)
	if poll := pollCreation(msg); poll != nil {
		body = poll.GetName()
		kind = "poll"
	}
	if contact := msg.GetContactMessage(); contact != nil {
		body = contact.GetDisplayName()
		kind = "contact"
	}
	if invite := msg.GetGroupInviteMessage(); invite != nil {
		body = invite.GetGroupName()
		if invite.GetCaption() != "" {
			body += " — " + invite.GetCaption()
		}
		kind = "group_invite"
	}
	if body == "" && kind == "" {
		return nil
	} // accepted protocol signal, not an invented text message
	sender := ""
	if c.wa.Store.ID != nil {
		sender = c.wa.Store.ID.ToNonAD().String()
	}
	return []store.MessageEvent{{Message: store.Message{ID: id, ChatJID: chat, Sender: sender, Content: body, IsFromMe: true, MediaType: kind, Filename: name, URL: url, DirectPath: direct, MentionJIDs: extractMentionJIDs(msg)}, ChatName: c.store.FindChatName(chat), PollOptions: extractPollOptionNames(pollCreation(msg)), MediaKey: key, FileSHA256: sha, FileEncSHA256: enc, FileLength: length}}
}
