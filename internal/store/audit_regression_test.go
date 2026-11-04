package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestContextIncludesSameSecondAndRejectsAmbiguousID(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	ts := time.Date(2026, 1, 12, 12, 0, 0, 0, time.UTC)
	chat := "audit-a@g.us"
	other := "audit-b@g.us"
	for _, jid := range []string{chat, other} {
		if err := s.StoreChat(jid, "Audit", ts); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"a", "b", "c"} {
		if err := s.StoreMessage(ctx, Message{ID: id, ChatJID: chat, Sender: "123", Content: "test", Timestamp: ts}, nil, nil, nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	matches, err := s.ListMessages(ctx, ListMessagesParams{ChatJID: chat, Query: "test", Limit: 1, IncludeContext: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 || matches[0].ID != "c" || matches[1].ID != "b" {
		t.Fatalf("unstable same-second expansion: %+v", matches)
	}
	result, err := s.GetMessageContext(ctx, "b", 1, 1, chat)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Before) != 1 || result.Before[0].ID != "a" || len(result.After) != 1 || result.After[0].ID != "c" {
		t.Fatalf("same-second neighbors omitted: %+v", result)
	}
	if err := s.StoreMessage(ctx, Message{ID: "b", ChatJID: other, Sender: "456", Content: "different chat", Timestamp: ts}, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetMessageContext(ctx, "b", 1, 1); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("expected ambiguous ID error, got %v", err)
	}
	result, err = s.GetMessageContext(ctx, "b", 1, 1, other)
	if err != nil {
		t.Fatal(err)
	}
	if result.Message.ChatJID != other || len(result.Before) != 0 || len(result.After) != 0 {
		t.Fatal("context crossed chat")
	}
}

func TestOldChatSyncCannotRegressActivityOrEraseName(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	ts := time.Now().UTC()
	jid := "audit-chat@g.us"
	if err := s.StoreChat(jid, "Current name", ts); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreChat(jid, "", ts.Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	chat, err := s.GetChat(ctx, jid, false)
	if err != nil {
		t.Fatal(err)
	}
	if chat.Name != "Current name" {
		t.Fatal("name erased")
	}
	var got time.Time
	if err := s.db.QueryRow("SELECT last_message_time FROM chats WHERE jid=?", jid).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !got.Equal(ts) {
		t.Fatal("history regressed last activity")
	}
}

func TestSenderFilterAcceptsBareAndFullJIDs(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	chat := "audit-filter@g.us"
	ts := time.Now().UTC()
	if err := s.StoreChat(chat, "Filter", ts); err != nil {
		t.Fatal(err)
	}
	for _, m := range []Message{
		{ID: "bare", ChatJID: chat, Sender: "123", Content: "test", Timestamp: ts},
		{ID: "full", ChatJID: chat, Sender: "123@s.whatsapp.net", Content: "test", Timestamp: ts},
		{ID: "other", ChatJID: chat, Sender: "9123", Content: "test", Timestamp: ts},
	} {
		if err := s.StoreMessage(ctx, m, nil, nil, nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	for _, filter := range []string{"123", "123@s.whatsapp.net"} {
		msgs, err := s.ListMessages(ctx, ListMessagesParams{ChatJID: chat, SenderPhone: filter})
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != 2 {
			t.Fatalf("filter %s returned %d messages, want 2", filter, len(msgs))
		}
	}
}
