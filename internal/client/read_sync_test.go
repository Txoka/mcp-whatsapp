package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sealjay/mcp-whatsapp/internal/store"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

func TestReadGateWaitsForCatchupAndImport(t *testing.T) {
	var s readSyncState
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.wait(ctx, func() bool { return true }) }()
	finish := s.processing()
	s.connection(true)
	select {
	case <-done:
		t.Fatal("returned during import")
	case <-time.After(10 * time.Millisecond):
	}
	finish(nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("did not wake")
	}
	finish = s.processing()
	finish(errors.New("disk failure"))
	if s.wait(ctx, func() bool { return true }) == nil {
		t.Fatal("storage failure hidden")
	}
	if s.wait(ctx, func() bool { return false }) == nil {
		t.Fatal("disconnected cache served")
	}
}
func TestHistoryCompletionMatchesRequestAndWaitsForFinalChunk(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := &Client{store: s}
	chat := "123@s.whatsapp.net"
	if err := s.StartHistory(context.Background(), "request", chat, store.HistoryAnchor{ID: "anchor", Timestamp: time.Unix(200, 0)}); err != nil {
		t.Fatal(err)
	}
	w := c.syncState.register("request", chat)
	evt := &events.HistorySync{Notification: &waE2E.HistorySyncNotification{PeerDataRequestSessionID: proto.String("other")}, Data: &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Progress: proto.Uint32(50), Conversations: []*waHistorySync.Conversation{{ID: proto.String(chat)}}}}
	c.completeHistory(evt, nil)
	evt.Notification.PeerDataRequestSessionID = proto.String("request")
	c.completeHistory(evt, nil)
	select {
	case <-w.done:
		t.Fatal("unrelated or partial chunk completed request")
	default:
	}
	evt.Data.Progress = proto.Uint32(100)
	evt.Data.Conversations[0].EndOfHistoryTransferType = waHistorySync.Conversation_COMPLETE_AND_NO_MORE_MESSAGE_REMAIN_ON_PRIMARY.Enum()
	c.completeHistory(evt, nil)
	select {
	case <-w.done:
		if !w.result.Exhausted {
			t.Fatal("lost explicit boundary")
		}
	default:
		t.Fatal("final chunk did not complete")
	}
}
func TestReadGateCancellation(t *testing.T) {
	var s readSyncState
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if s.wait(ctx, func() bool { return true }) == nil {
		t.Fatal("cancel ignored")
	}
}

func TestMissingCompletionMetadataDoesNotComplete(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := &Client{store: s}
	if err := s.StartHistory(context.Background(), "request", "123@s.whatsapp.net", store.HistoryAnchor{ID: "anchor", Timestamp: time.Unix(200, 0)}); err != nil {
		t.Fatal(err)
	}
	w := c.syncState.register("request", "123@s.whatsapp.net")
	evt := &events.HistorySync{Notification: &waE2E.HistorySyncNotification{PeerDataRequestSessionID: proto.String("request")}, Data: &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Conversations: []*waHistorySync.Conversation{{ID: proto.String("123@s.whatsapp.net")}}}}
	c.completeHistory(evt, nil)
	select {
	case <-w.done:
		t.Fatal("missing metadata was guessed to mean completion")
	default:
	}
}

func TestReadCollectionFailuresAreScopedAndNotClearedByReconnect(t *testing.T) {
	var state readSyncState
	state.connection(true)
	state.collectionFailure("regular_low", errors.New("read-state sync failed"))
	if err := state.wait(context.Background(), func() bool { return true }, "chat", "messages"); err != nil {
		t.Fatal("unrelated read state blocked message content", err)
	}
	state.connection(false)
	state.connection(true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := state.wait(ctx, func() bool { return true }, "chat", "unread"); err == nil {
		t.Fatal("reconnect falsely cleared a collection failure")
	}
	state.collectionComplete("regular_low")
	if err := state.wait(context.Background(), func() bool { return true }, "chat", "unread"); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryCompletionRetainsEarlierChunkMembership(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	chat := "123@s.whatsapp.net"
	if err = s.StartHistory(ctx, "chunks", chat, store.HistoryAnchor{ID: "head", Timestamp: time.Unix(200, 0)}); err != nil {
		t.Fatal(err)
	}
	c := &Client{store: s}
	w := c.syncState.register("chunks", chat)
	makeEvent := func(key string, stamp int64, progress uint32) *events.HistorySync {
		conv := &waHistorySync.Conversation{}
		raw := fmt.Sprintf(`{"id":%q,"messages":[{"message":{"key":{"ID":%q},"messageTimestamp":%d}}]}`, chat, key, stamp)
		if err := json.Unmarshal([]byte(raw), conv); err != nil {
			t.Fatal(err)
		}
		if len(conv.Messages) != 1 || conv.Messages[0].Message.GetKey().GetID() != key {
			t.Fatal("invalid history test fixture")
		}
		return &events.HistorySync{Notification: &waE2E.HistorySyncNotification{PeerDataRequestSessionID: proto.String("chunks")}, Data: &waHistorySync.HistorySync{SyncType: waHistorySync.HistorySync_ON_DEMAND.Enum(), Progress: proto.Uint32(progress), Conversations: []*waHistorySync.Conversation{conv}}}
	}
	c.completeHistory(makeEvent("early", 100, 50), nil)
	select {
	case <-w.done:
		t.Fatal("partial chunk completed request")
	default:
	}
	c.completeHistory(makeEvent("late", 150, 100), nil)
	select {
	case <-w.done:
		if w.err != nil {
			t.Fatal(w.err)
		}
	default:
		t.Fatal("final chunk did not complete")
	}
	var count int
	if err = s.DB().QueryRow("SELECT COUNT(*) FROM history_members WHERE request_id='chunks'").Scan(&count); err != nil || count != 3 {
		t.Fatalf("earlier chunk lost: %d %v", count, err)
	}
	coverage, _, err := s.HistoryCoverage(ctx, chat, "head")
	if err != nil || coverage.LowerID != "early" {
		t.Fatal("frontier moved forwards to final chunk", err)
	}
}
