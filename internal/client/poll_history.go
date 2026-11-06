package client

import (
	"bytes"
	"context"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waWeb"
)

// historyPollVotes imports decoded vote observations carried with the original
// poll in phone history. Sender clocks are not used as authoritative revisions.
func (c *Client) historyPollVotes(ctx context.Context, rawChat, chat, ownID string, message *waWeb.WebMessageInfo, options []string) []store.MessageEvent {
	if message == nil || message.GetKey().GetID() == "" || len(message.GetPollUpdates()) == 0 {
		return nil
	}
	pollID := message.GetKey().GetID()
	if len(options) == 0 {
		var err error
		options, err = c.store.GetPollOptions(ctx, pollID, chat)
		if err != nil {
			return nil
		}
	}
	hashes := whatsmeow.HashPollOptions(options)
	var projected []store.MessageEvent
	for _, update := range message.GetPollUpdates() {
		if update == nil || update.GetVote() == nil || update.GetServerTimestampMS() <= 0 {
			continue
		}
		key := update.GetPollUpdateMessageKey()
		voter, _ := historySender(rawChat, ownID, key.GetFromMe(), key.GetParticipant(), "")
		if voter == "" {
			continue
		}
		voter = c.store.ResolveLIDToJID(voter)
		selected := make([]string, 0, len(update.GetVote().GetSelectedOptions()))
		known := true
		for _, selectedHash := range update.GetVote().GetSelectedOptions() {
			matched := false
			for i, hash := range hashes {
				if bytes.Equal(hash, selectedHash) {
					selected = append(selected, options[i])
					matched = true
					break
				}
			}
			known = known && matched
		}
		// An unknown option is not a withdrawal. Wait for matching poll metadata.
		if !known {
			continue
		}
		event, err := store.NewPollVoteEvent(pollID, chat, voter, selected, time.UnixMilli(update.GetServerTimestampMS()))
		if err == nil {
			projected = append(projected, event)
		}
	}
	return projected
}
