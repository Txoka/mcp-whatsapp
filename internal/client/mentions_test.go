package client

import (
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"testing"
	"time"
)

func TestNormalizePreservesExplicitMentions(t *testing.T) {
	s := newTestStoreWithLIDMap(t, nil)
	c := newClientWithStore(t, s)
	raw := &waProto.Message{ExtendedTextMessage: &waProto.ExtendedTextMessage{ContextInfo: &waProto.ContextInfo{MentionedJID: []string{"777@lid"}}}}
	normalized := c.normalizeIncomingMessage("123@g.us", "456@s.whatsapp.net", "456", true, false, "111", "mention", time.Now(), raw)
	if len(normalized.msg.MentionJIDs) != 1 || normalized.msg.MentionJIDs[0] != "777@lid" {
		t.Fatal("mention metadata dropped")
	}
	if len(extractMentionJIDs(nil)) != 0 {
		t.Fatal("nil message produced mentions")
	}
}
