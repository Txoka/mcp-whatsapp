package client

import (
	"go.mau.fi/whatsmeow/appstate"
	"testing"
	"time"
)

func TestDeleteForMePatch(t *testing.T) {
	p := deleteForMePatch("123@g.us", "456@s.whatsapp.net", "id", "0", time.Unix(123456, 0))
	if p.Type != appstate.WAPatchRegularHigh || len(p.Mutations) != 1 {
		t.Fatal("wrong patch")
	}
	m := p.Mutations[0]
	if m.Version != 3 || len(m.Index) != 5 || m.Index[0] != appstate.IndexDeleteMessageForMe || m.Index[3] != "0" || m.Index[4] != "456@s.whatsapp.net" {
		t.Fatalf("wrong mutation: %+v", m)
	}
	if m.Value.GetDeleteMessageForMeAction().GetMessageTimestamp() != 123456 {
		t.Fatal("wrong timestamp")
	}
}
