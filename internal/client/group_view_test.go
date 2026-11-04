package client

import (
	"database/sql"
	"go.mau.fi/whatsmeow/types"
	"path/filepath"
	"testing"
)

func TestParticipantNamesAreSeparatedAndAliasesResolved(t *testing.T) {
	s := newTestStoreWithLIDMap(t, map[string]string{"777": "111"})
	c := newClientWithStore(t, s)
	db, err := sql.Open("sqlite3", filepath.Join(s.Dir(), "whatsapp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE whatsmeow_contacts(their_jid TEXT,full_name TEXT,first_name TEXT,push_name TEXT,business_name TEXT);INSERT INTO whatsmeow_contacts VALUES('111@s.whatsapp.net','Saved Alice','','Chosen Alice','')`); err != nil {
		t.Fatal(err)
	}
	member := types.GroupParticipant{JID: types.NewJID("777", "lid"), LID: types.NewJID("777", "lid"), IsAdmin: true, DisplayName: "anonymous-mask"}
	got := c.participantView(member)
	if got.Phone == nil || *got.Phone != "+111" || got.ContactName == nil || *got.ContactName != "Saved Alice" || got.ProfileName == nil || *got.ProfileName != "Chosen Alice" || got.Role != "admin" {
		t.Fatalf("bad identity: %+v", got)
	}
	unknown := c.participantView(types.GroupParticipant{JID: types.NewJID("888", "lid"), DisplayName: "masked"})
	if unknown.ProfileName != nil || unknown.ContactName != nil || unknown.Phone != nil {
		t.Fatal("unknown or anonymous identity invented")
	}
	info := testGroupInfo(true)
	first := c.groupView(info, 2, 0)
	second := c.groupView(info, 2, 1)
	if len(first.Participants) != 2 || !first.HasMore || len(second.Participants) != 1 || second.HasMore {
		t.Fatal("participant pagination failed")
	}
	if len(c.groupView(info, 2, 100).Participants) != 0 {
		t.Fatal("out of range page not empty")
	}
}
