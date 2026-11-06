package client

import (
	"context"
	"testing"
	"time"

	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"google.golang.org/protobuf/proto"
)

func TestHistoryPollVotesUseServerRevisionsRegardlessOfArrivalOrder(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := &Client{store: s}
	ctx := context.Background()
	chat, poll := "123@g.us", "poll"
	ts := time.Date(2026, 11, 6, 18, 18, 53, 0, time.UTC)
	options := []string{"a", "b"}
	hashes := whatsmeow.HashPollOptions(options)
	update := func(ms int64, selected [][]byte) *waWeb.PollUpdate {
		return &waWeb.PollUpdate{
			PollUpdateMessageKey: &waCommon.MessageKey{Participant: proto.String("456@s.whatsapp.net")},
			Vote:                 &waE2E.PollVoteMessage{SelectedOptions: selected},
			ServerTimestampMS:    proto.Int64(ms),
			SenderTimestampMS:    proto.Int64(ts.Add(365 * 24 * time.Hour).UnixMilli()),
		}
	}
	message := &waWeb.WebMessageInfo{
		Key: &waCommon.MessageKey{ID: proto.String(poll)},
		// A newer withdrawal precedes an older vote in the transfer.
		PollUpdates: []*waWeb.PollUpdate{
			update(ts.Add(200*time.Millisecond).UnixMilli(), nil),
			update(ts.Add(100*time.Millisecond).UnixMilli(), hashes[:1]),
		},
	}
	events := []store.MessageEvent{{Message: store.Message{ID: poll, ChatJID: chat, Content: "[poll] choose", Timestamp: ts}, PollOptions: options}}
	votes := c.historyPollVotes(ctx, chat, chat, "100", message, options)
	if len(votes) != 2 || !votes[0].Message.Timestamp.Equal(ts.Add(200*time.Millisecond)) {
		t.Fatalf("server millisecond ordering was lost: %+v", votes)
	}
	if _, err := s.ApplyHistory(ctx, append(events, votes...), nil); err != nil {
		t.Fatal(err)
	}
	_, tally, err := s.GetPollResults(ctx, poll, chat)
	if err != nil || len(tally) != 2 || tally[0].Votes != 0 || tally[1].Votes != 0 {
		t.Fatalf("history did not reconcile withdrawal: %+v, %v", tally, err)
	}
	// Unknown hashes, unknown group authors, and missing server times cannot
	// become invented withdrawals or revisions based on a peer's future clock.
	badHash := update(ts.Add(time.Second).UnixMilli(), [][]byte{[]byte("unknown")})
	missingAuthor := update(ts.Add(time.Second).UnixMilli(), nil)
	missingAuthor.PollUpdateMessageKey = nil
	missingTime := update(0, nil)
	message.PollUpdates = []*waWeb.PollUpdate{badHash, missingAuthor, missingTime}
	if got := c.historyPollVotes(ctx, chat, chat, "100", message, options); len(got) != 0 {
		t.Fatalf("invalid metadata generated votes: %+v", got)
	}
}
