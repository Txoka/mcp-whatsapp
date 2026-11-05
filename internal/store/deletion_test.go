package store

import (
	"context"
	"testing"
	"time"
)

func TestDeleteForMeDoesNotReturnAfterHistoryReplay(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.db.Exec(schema + mentionsSchema + updatesSchema); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	chat := "447700000001@s.whatsapp.net"
	m := Message{ID: "deleted-test", ChatJID: chat, Sender: "447700000001", Content: "secret", Timestamp: time.Now()}
	if err := s.StoreMessage(ctx, m, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteForMe(ctx, chat, m.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteForMe(ctx, chat, m.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreMessage(ctx, m, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetMessageContext(ctx, m.ID, 0, 0, chat); err == nil {
		t.Fatal("deleted message returned after replay")
	}
	m.ChatJID = "447700000002@s.whatsapp.net"
	if err := s.StoreMessage(ctx, m, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM messages WHERE id=?", m.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("deletion leaked across chats: %d %v", count, err)
	}
}
