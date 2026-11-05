package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"strings"
)

const bookmarkSchema = `CREATE TABLE IF NOT EXISTS cursor_bookmarks (
 token TEXT PRIMARY KEY, kind TEXT NOT NULL, scope TEXT NOT NULL, payload TEXT NOT NULL,
 UNIQUE(kind,scope,payload));`

// PutBookmark persists an opaque continuation reference. References contain no
// message IDs or query data, survive restarts, and are not authorization tokens.
func (s *Store) PutBookmark(ctx context.Context, kind, scope, payload string) (string, error) {
	if payload == "" {
		return "", nil
	}
	if kind != "p" && kind != "u" {
		return "", fmt.Errorf("cursor: invalid bookmark kind")
	}
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	token := kind + "_" + base64.RawURLEncoding.EncodeToString(entropy[:])
	if _, err := s.db.ExecContext(ctx, "INSERT INTO cursor_bookmarks(token,kind,scope,payload) VALUES(?,?,?,?) ON CONFLICT(kind,scope,payload) DO NOTHING", token, kind, scope, payload); err != nil {
		return "", err
	}
	err := s.db.QueryRowContext(ctx, "SELECT token FROM cursor_bookmarks WHERE kind=? AND scope=? AND payload=?", kind, scope, payload).Scan(&token)
	return token, err
}

// ResolveBookmark requires a persisted reference matching its purpose and
// original filters. Missing references fail
// explicitly instead of silently restarting pagination or a watch.
func (s *Store) ResolveBookmark(ctx context.Context, kind, scope, token string) (string, error) {
	if token == "" {
		return "", nil
	}
	if !strings.HasPrefix(token, kind+"_") || len(token) != 24 {
		return "", fmt.Errorf("cursor: wrong kind or invalid bookmark")
	}
	var payload string
	err := s.db.QueryRowContext(ctx, "SELECT payload FROM cursor_bookmarks WHERE token=? AND kind=? AND scope=?", token, kind, scope).Scan(&payload)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("cursor: unknown bookmark or filters changed; reuse the original filters")
	}
	return payload, err
}
