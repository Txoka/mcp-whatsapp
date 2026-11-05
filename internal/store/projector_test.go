package store

import (
	"context"
	"testing"
	"time"
)

func TestProjectionReplayCannotUndoEditOrDeletion(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	chat := "987@g.us"
	m := Message{ID: "m", ChatJID: chat, Sender: "123", Content: "original", Timestamp: time.Now()}
	edit := m
	edit.Content = "edited @123"
	edit.MentionJIDs = []string{"123@s.whatsapp.net"}
	apply := func(events ...MessageEvent) {
		t.Helper()
		if _, err := s.ApplyMessages(ctx, events); err != nil {
			t.Fatal(err)
		}
	}
	// The edit arrives before its original creation event.
	apply(MessageEvent{Kind: "edit", Message: edit, RevisionMS: 2000})
	apply(MessageEvent{Message: m, Incoming: true})
	apply(MessageEvent{Kind: "edit", Message: m, RevisionMS: 1000})
	apply(MessageEvent{Message: m})
	loaded, err := s.GetMessageContext(ctx, m.ID, 0, 0, chat)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Message.Content != edit.Content || !loaded.Message.Timestamp.Equal(m.Timestamp) {
		t.Fatal("history/older edit replaced newer content or creation time")
	}
	var mentions int
	s.db.QueryRow("SELECT COUNT(*) FROM message_mentions WHERE chat_jid=? AND message_id=?", chat, m.ID).Scan(&mentions)
	if mentions != 1 {
		t.Fatal("pending edit mentions lost")
	}
	apply(MessageEvent{Kind: "revoke", Message: m})
	apply(MessageEvent{Message: m, Incoming: true}, MessageEvent{Kind: "edit", Message: edit, RevisionMS: 3000})
	for _, table := range []string{"messages", "message_mentions", "message_read_state", "local_updates", "message_revisions"} {
		var count int
		query := "SELECT COUNT(*) FROM " + table + " WHERE chat_jid=?"
		if err = s.db.QueryRow(query, chat).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("deleted message leaked into %s", table)
		}
	}
}
func TestProjectionBatchRollsBackAllTables(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	chat := "rollback@g.us"
	m := Message{ID: "m", ChatJID: chat, Content: "text", Timestamp: time.Now(), MentionJIDs: []string{"123@s.whatsapp.net"}}
	_, err := s.ApplyMessages(ctx, []MessageEvent{{Message: m, Incoming: true}, {Kind: "edit", Message: m}})
	if err == nil {
		t.Fatal("missing revision accepted")
	}
	for _, table := range []string{"messages", "chats", "message_mentions", "message_read_state", "local_updates", "projection_events"} {
		var n int
		column := "chat_jid"
		if table == "chats" {
			column = "jid"
		}
		if err := s.db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE "+column+"=?", chat).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("partial transaction visible in %s", table)
		}
	}
}
func TestProjectionDuplicateAndDeletionUnreadCounter(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	chat := "counter@g.us"
	zero := uint32(0)
	if err := s.SeedUnread(chat, &zero, false); err != nil {
		t.Fatal(err)
	}
	m := Message{ID: "m", ChatJID: chat, Sender: "123", Content: "text", Timestamp: time.Now()}
	for i := 0; i < 2; i++ {
		if _, err := s.ApplyMessages(ctx, []MessageEvent{{Message: m, Incoming: true}}); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	s.db.QueryRow("SELECT unread_count FROM chat_read_state WHERE chat_jid=?", chat).Scan(&count)
	if count != 1 {
		t.Fatalf("duplicate unread count %d", count)
	}
	if err := s.DeleteForMe(ctx, chat, m.ID); err != nil {
		t.Fatal(err)
	}
	s.db.QueryRow("SELECT unread_count FROM chat_read_state WHERE chat_jid=?", chat).Scan(&count)
	if count != 0 {
		t.Fatalf("deleted unread count %d", count)
	}
}

func TestUnversionedConflictIsNotPresentedAsKnownText(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	m := Message{ID: "conflict", ChatJID: "conflict@g.us", Content: "original", Timestamp: time.Now()}
	if err := s.StoreMessage(ctx, m, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	m.Content = "different unversioned copy"
	if err := s.StoreMessage(ctx, m, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	result, err := s.GetMessageContext(ctx, m.ID, 0, 0, m.ChatJID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Message.Content == "original" || result.Message.Content == m.Content {
		t.Fatal("conflicting content presented as authoritative")
	}
	if _, err = s.ListMessages(ctx, ListMessagesParams{ChatJID: m.ChatJID, Query: "different"}); err == nil {
		t.Fatal("search claimed an answer over unknown content")
	}
	m.Content = "confirmed edit"
	if _, err = s.ApplyMessages(ctx, []MessageEvent{{Kind: "edit", Message: m, RevisionMS: 2000}}); err != nil {
		t.Fatal(err)
	}
	result, err = s.GetMessageContext(ctx, m.ID, 0, 0, m.ChatJID)
	if err != nil || result.Message.Content != m.Content {
		t.Fatal("known edit did not resolve conflict")
	}
}

func TestLegacyMissingCaptionCanBeRepairedByHistory(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	m := Message{ID: "caption", ChatJID: "caption@g.us", Timestamp: time.Now(), MediaType: "image"}
	if _, err := s.ApplyMessages(ctx, []MessageEvent{{Message: m, ContentUnknown: true}}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetMessageContext(ctx, m.ID, 0, 0, m.ChatJID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Message.ContentStatus != "unknown" {
		t.Fatal("invented empty caption")
	}
	m.Content = "Actual caption"
	if _, err = s.ApplyMessages(ctx, []MessageEvent{{Message: m}}); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetMessageContext(ctx, m.ID, 0, 0, m.ChatJID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Message.ContentStatus != "known" || got.Message.Content != m.Content {
		t.Fatal("caption was not recovered")
	}
}
