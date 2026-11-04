package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestHistoryContextUniqueOrderedAndScoped(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	messages, err := s.ListMessages(ctx, ListMessagesParams{ChatJID: "447700000001@s.whatsapp.net", Limit: 50, IncludeContext: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 5 {
		t.Fatalf("want five unique messages, got %d", len(messages))
	}
	seen := map[string]bool{}
	for i, m := range messages {
		if seen[m.ID] {
			t.Fatal("duplicate ID")
		}
		seen[m.ID] = true
		if m.ChatJID != "447700000001@s.whatsapp.net" {
			t.Fatal("context crossed chat")
		}
		if i > 0 && m.Timestamp.After(messages[i-1].Timestamp) {
			t.Fatal("wrong order")
		}
	}
}
func TestUnknownHistoryPreservesAuthor(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	m := Message{ID: "history-update", ChatJID: "999@g.us", Sender: "123", Content: "test", Timestamp: time.Now(), IsFromMe: true}
	if _, err := s.db.Exec("INSERT INTO chats(jid,name) VALUES('999@g.us','Group')"); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreMessage(ctx, m, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	m.Sender = "999"
	m.IsFromMe = false
	if err := s.StoreMessage(ctx, m, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	var sender string
	var mine bool
	if err := s.db.QueryRow("SELECT sender,is_from_me FROM messages WHERE id=?", m.ID).Scan(&sender, &mine); err != nil {
		t.Fatal(err)
	}
	if sender != "123" || !mine {
		t.Fatalf("lost known author: %s %v", sender, mine)
	}
	m.ID = "unknown-new"
	if err := s.StoreMessage(ctx, m, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow("SELECT sender FROM messages WHERE id=?", m.ID).Scan(&sender); err != nil {
		t.Fatal(err)
	}
	if sender != "" {
		t.Fatal("group became sender")
	}
	output := s.FormatMessage(ctx, m, true)
	for _, want := range []string{"ID: unknown-new", "TZ:", "Sender: unknown", "From: Unknown participant:"} {
		if !strings.Contains(output, want) {
			t.Fatalf("missing %s", want)
		}
	}
	if s.GetSenderName(ctx, "999") != "999" {
		t.Fatal("substring resolved a group as a participant")
	}
	if s.GetSenderName(ctx, "447700000001") != "Alice" {
		t.Fatal("bare sender failed direct-chat resolution")
	}
}
