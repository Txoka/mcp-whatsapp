package store

import "database/sql"

// contactNameExpr coalesces the whatsmeow_contacts name columns in preference
// order: the saved address-book name first, then push name, first name, and
// business name. NULLIF folds empty strings into NULL so COALESCE skips them.
const contactNameExpr = `COALESCE(NULLIF(full_name,''), NULLIF(push_name,''), NULLIF(first_name,''), NULLIF(business_name,''))`

// ContactName returns the synced address-book name for a JID from the
// whatsmeow_contacts table, or "" when the contact is unknown or the
// whatsmeow store is unavailable. LID JIDs are resolved to phone JIDs first.
func (s *Store) ContactName(jid string) string {
	if s.whatsmeowDB == nil {
		return ""
	}
	jid = s.ResolveLIDToJID(jid)
	var name sql.NullString
	err := s.whatsmeowDB.QueryRow(
		`SELECT `+contactNameExpr+` FROM whatsmeow_contacts WHERE their_jid = ?`, jid).Scan(&name)
	if err != nil || !name.Valid {
		return ""
	}
	return name.String
}

// searchContactNames returns address-book contacts whose name or JID matches
// the LIKE pattern, as a jid -> display-name map. Empty when the whatsmeow
// store is unavailable.
func (s *Store) searchContactNames(pattern string) map[string]string {
	out := map[string]string{}
	if s.whatsmeowDB == nil {
		return out
	}
	rows, err := s.whatsmeowDB.Query(`
		SELECT their_jid, `+contactNameExpr+` AS name
		FROM whatsmeow_contacts
		WHERE their_jid LIKE '%@s.whatsapp.net'
		  AND (LOWER(full_name) LIKE LOWER(?) OR LOWER(push_name) LIKE LOWER(?)
		    OR LOWER(first_name) LIKE LOWER(?) OR LOWER(business_name) LIKE LOWER(?)
		    OR their_jid LIKE ?)
		LIMIT 50`, pattern, pattern, pattern, pattern, pattern)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var jid string
		var name sql.NullString
		if err := rows.Scan(&jid, &name); err != nil {
			continue
		}
		if name.Valid && name.String != "" {
			out[jid] = name.String
		}
	}
	return out
}

// ContactNames keeps saved address-book and self-chosen push names separate.
// Exact PN/LID aliases are accepted; no fuzzy phone-number matching is used.
func (s *Store) ContactNames(jids ...string) (contact, profile string) {
	if s.whatsmeowDB == nil {
		return
	}
	seen := map[string]bool{}
	for _, raw := range jids {
		for _, jid := range []string{s.ResolveLIDToJID(raw), raw} {
			if jid == "" || seen[jid] {
				continue
			}
			seen[jid] = true
			var full, first, push sql.NullString
			if s.whatsmeowDB.QueryRow("SELECT full_name,first_name,push_name FROM whatsmeow_contacts WHERE their_jid=?", jid).Scan(&full, &first, &push) != nil {
				continue
			}
			if contact == "" {
				contact = full.String
				if contact == "" {
					contact = first.String
				}
			}
			if profile == "" {
				profile = push.String
			}
		}
	}
	return
}
