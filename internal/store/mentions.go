package store

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
)

const mentionsSchema = `CREATE TABLE IF NOT EXISTS message_mentions(chat_jid TEXT NOT NULL,message_id TEXT NOT NULL,jids_json TEXT NOT NULL,PRIMARY KEY(chat_jid,message_id));`

var mentionToken = regexp.MustCompile(`@[0-9]+`)

type Mention struct {
	Name string `json:"name"`
	IsMe bool   `json:"is_me"`
	JID  string `json:"jid,omitempty"`
}

func (s *Store) SetOwnIdentity(phone, lid string) error {
	b, _ := json.Marshal([]string{phone, lid})
	_, err := s.db.Exec("INSERT OR REPLACE INTO local_metadata(key,value) VALUES('own_mention_identity',?)", string(b))
	return err
}
func (s *Store) RenderMentions(ctx context.Context, m Message, detailed bool) (string, []Mention, bool) {
	jids := m.MentionJIDs
	if len(jids) == 0 {
		var raw string
		if s.reader(ctx).QueryRowContext(ctx, "SELECT jids_json FROM message_mentions WHERE chat_jid=? AND message_id=?", m.ChatJID, m.ID).Scan(&raw) == nil {
			json.Unmarshal([]byte(raw), &jids)
		}
	}
	if len(jids) == 0 {
		return m.Content, nil, false
	}
	var ownRaw string
	var own []string
	if s.reader(ctx).QueryRowContext(ctx, "SELECT value FROM local_metadata WHERE key='own_mention_identity'").Scan(&ownRaw) == nil {
		json.Unmarshal([]byte(ownRaw), &own)
	}
	ownSet := map[string]bool{}
	for _, jid := range own {
		if jid != "" {
			ownSet[jid] = true
			ownSet[s.ResolveLIDToJID(jid)] = true
		}
	}
	names := map[string]string{}
	seen := map[string]bool{}
	var mentions []Mention
	mentionsMe := false
	for _, jid := range jids {
		if !strings.Contains(jid, "@") {
			continue
		}
		resolved := s.ResolveLIDToJID(jid)
		isMe := ownSet[jid] || ownSet[resolved]
		name := s.ContactName(resolved)
		if name == "" {
			name = s.chatNameByJID(ctx, resolved)
			if name == jidUser(resolved) {
				name = ""
			}
		}
		if name == "" {
			if isMe {
				name = "You"
			} else {
				name = "Unknown person"
			}
		}
		name = strings.NewReplacer("\n", " ", "\r", " ").Replace(name)
		names["@"+jidUser(jid)] = name
		names["@"+jidUser(resolved)] = name
		mentionsMe = mentionsMe || isMe
		if !seen[resolved] {
			item := Mention{Name: name, IsMe: isMe}
			if detailed {
				item.JID = jid
			}
			mentions = append(mentions, item)
			seen[resolved] = true
		}
	}
	text := mentionToken.ReplaceAllStringFunc(m.Content, func(token string) string {
		if name, ok := names[token]; ok {
			return "@" + name
		}
		return token
	})
	return text, mentions, mentionsMe
}
