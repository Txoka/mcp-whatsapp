package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestMentionMetadataResolutionAndRawTextPreserved(t *testing.T) {
	s := updatesStore(t)
	ctx := context.Background()
	chat := "123@g.us"
	if err := s.SetOwnIdentity("111@s.whatsapp.net", "777@lid"); err != nil {
		t.Fatal(err)
	}
	m := incoming(t, s, chat, "mention", time.Now())
	m.Content = "@777 help @999 and literal @7777"
	m.MentionJIDs = []string{"777@lid", "999@lid", "777@lid"}
	if err := s.StoreMessage(ctx, m, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	loaded := m
	loaded.MentionJIDs = nil
	text, mentions, me := s.RenderMentions(ctx, loaded, false)
	if text != "@You help @Unknown person and literal @7777" || !me || len(mentions) != 2 || !mentions[0].IsMe || mentions[0].JID != "" {
		t.Fatalf("bad resolution: %s %+v %v", text, mentions, me)
	}
	_, detail, _ := s.RenderMentions(ctx, loaded, true)
	if detail[0].JID != "777@lid" {
		t.Fatal("detailed reference unavailable")
	}
	var raw string
	if err := s.DB().QueryRow("SELECT content FROM messages WHERE id=?", m.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw != m.Content {
		t.Fatal("raw content changed")
	}
	out, err := s.FormatMessagesCompact(ctx, []Message{loaded}, "Europe/Madrid")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "[mentions you] @You") {
		t.Fatal("compact mention missing")
	}
	plain := Message{ID: "plain", ChatJID: chat, Content: "@777 literal"}
	text, mentions, me = s.RenderMentions(ctx, plain, false)
	if text != plain.Content || len(mentions) > 0 || me {
		t.Fatal("invented metadata from text")
	}
}

func TestMentionsResolveKnownLIDAndDeduplicateAliases(t *testing.T) {
	s := openTestStore(t)
	m := Message{Content: "@11223344 and @919900000000", MentionJIDs: []string{"11223344@lid", "919900000000@s.whatsapp.net"}}
	text, mentions, me := s.RenderMentions(context.Background(), m, false)
	if text != "@Jane Smith Delhi and @Jane Smith Delhi" || len(mentions) != 1 || me {
		t.Fatalf("bad contact resolution: %s %+v", text, mentions)
	}
}
