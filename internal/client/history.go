package client

import (
	"context"
	"fmt"

	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// requestHistorySync submits a correlated request anchored on an exact stored key.
// Completion and data coverage are tracked separately by FetchHistory.
func (c *Client) requestHistorySync(ctx context.Context, chatJID string, anchor store.HistoryAnchor, count int, useLID bool, requestID types.MessageID) error {

	if c.wa == nil || !c.wa.IsConnected() {
		return fmt.Errorf("client not connected")
	}
	if c.wa.Store.ID == nil {
		return fmt.Errorf("device not paired")
	}

	jid, err := types.ParseJID(chatJID)
	if err != nil {
		return fmt.Errorf("invalid JID format: %v", err)
	}

	if count <= 0 || anchor.ID == "" || anchor.Timestamp.IsZero() || requestID == "" {
		return fmt.Errorf("history request requires a real anchor, correlation ID and positive count")
	}
	msgID, timestamp, isFromMe := anchor.ID, anchor.Timestamp, anchor.IsFromMe

	// The phone may use the LID form internally; resolve it for direct chats
	// so the server can match the chat the anchor key belongs to.
	//
	// This is era-sensitive: messages predating WhatsApp's LID migration are
	// keyed on the phone-number JID, so requesting them with the LID form
	// yields a conversation with zero messages. Callers walking back past the
	// migration boundary should set useLID=false.
	requestJID := jid
	if useLID && jid.Server == types.DefaultUserServer {
		if lidJID, lerr := c.wa.Store.LIDs.GetLIDForPN(ctx, jid); lerr == nil && !lidJID.IsEmpty() {
			c.log.Infof("[SYNC] Found LID mapping: %s -> %s", jid.String(), lidJID.String())
			requestJID = lidJID
		} else {
			c.log.Infof("[SYNC] No LID mapping found for %s, using original JID", jid.String())
		}
	} else if !useLID {
		c.log.Infof("[SYNC] LID resolution disabled; requesting with phone-number JID %s", jid.String())
	}

	messageInfo := &types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     requestJID,
			IsFromMe: isFromMe,
		},
		ID:        msgID,
		Timestamp: timestamp,
	}

	historyMsg := c.wa.BuildHistorySyncRequest(messageInfo, count)
	if historyMsg == nil {
		return fmt.Errorf("failed to build history sync request")
	}

	ownJID := c.wa.Store.ID.ToNonAD()
	c.log.Infof("[SYNC] Sending peer message to own JID: %s", ownJID.String())
	c.log.Infof("[SYNC] Request details: ChatJID=%s (request uses %s), MsgID=%s, IsFromMe=%v, Timestamp=%v, Count=%d",
		chatJID, requestJID.String(), msgID, isFromMe, timestamp, count)

	resp, err := c.wa.SendMessage(ctx, ownJID, historyMsg, whatsmeow.SendRequestExtra{Peer: true, ID: requestID})
	if err != nil {
		return fmt.Errorf("failed to send sync request: %v", err)
	}
	c.log.Infof("[SYNC] Send response: ID=%s, Timestamp=%v", resp.ID, resp.Timestamp)

	return nil
}
