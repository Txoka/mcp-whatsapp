package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCompactReadableTimezoneAndDetailedEscapeHatch(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	messages := []Message{
		{ID: "long-transport-id-a", ChatJID: "919900000000@s.whatsapp.net", ChatName: "WRONG-OWN-NUMBER", Sender: "919900000000", Content: "hello", Timestamp: time.Date(2026, 11, 4, 15, 35, 0, 0, time.UTC)},
		{ID: "long-transport-id-b", ChatJID: "919900000000@s.whatsapp.net", ChatName: "WRONG-OWN-NUMBER", IsFromMe: true, Content: "yes\nsecond line", MediaType: "image", Timestamp: time.Date(2026, 11, 4, 15, 34, 0, 0, time.UTC)},
	}
	got, err := s.FormatMessagesCompact(ctx, messages, "Europe/Madrid")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Jane Smith Delhi | 2026-11-04 | Europe/Madrid (UTC+01:00)", "16:35 Jane Smith Delhi: hello", "16:34 You: [image] yes\n  second line"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q: %s", want, got)
		}
	}
	for _, hidden := range []string{"long-transport-id", "@s.whatsapp.net", "WRONG-OWN-NUMBER", "Sender:", "TZ:"} {
		if strings.Contains(got, hidden) {
			t.Fatalf("unexpected transport metadata %s", hidden)
		}
	}
	if strings.Count(got, "Europe/Madrid") != 1 {
		t.Fatal("repeated header")
	}
	if !strings.Contains(s.FormatMessagesList(ctx, messages, true), "long-transport-id-a") {
		t.Fatal("detailed IDs unavailable")
	}
	if _, err := s.FormatMessagesCompact(ctx, messages, "invalid-zone"); err == nil {
		t.Fatal("invalid timezone accepted")
	}
	messages[0].Timestamp = time.Date(2026, 7, 4, 15, 35, 0, 0, time.UTC)
	summer, err := s.FormatMessagesCompact(ctx, messages[:1], "Europe/Madrid")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(summer, "UTC+02:00") || !strings.Contains(summer, "17:35") {
		t.Fatal("DST conversion failed")
	}
}

func TestChatMetadataPrefersPeerContactOverStaleCachedName(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	jid := "919900000000@s.whatsapp.net"
	if err := s.StoreChat(jid, "wrong sender name", time.Now()); err != nil {
		t.Fatal(err)
	}
	chat, err := s.GetChat(ctx, jid, false)
	if err != nil {
		t.Fatal(err)
	}
	if chat.Name != "Jane Smith Delhi" {
		t.Fatalf("wrong peer name: %s", chat.Name)
	}
}
