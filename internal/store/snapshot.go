package store

import (
	"context"
	"database/sql"
	"time"
)

type sqlReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}
type snapshotKey struct{ store *Store }
type Snapshot struct {
	Watermark int64
	At        time.Time
}

func (s *Store) reader(ctx context.Context) sqlReader {
	if tx, ok := ctx.Value(snapshotKey{s}).(*sql.Tx); ok {
		return tx
	}
	return s.db
}

// ReadSnapshot pins all context-aware projection reads to one SQLite snapshot.
// SDK contact lookups are a separate source and are not a phone-state snapshot.
func (s *Store) ReadSnapshot(ctx context.Context, read func(context.Context) error) (Snapshot, error) {
	var snapshot Snapshot
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return snapshot, err
	}
	defer tx.Rollback()
	if err = tx.QueryRowContext(ctx, "SELECT version FROM projection_clock WHERE id=1").Scan(&snapshot.Watermark); err != nil {
		return snapshot, err
	}
	snapshot.At = time.Now().UTC()
	if err = read(context.WithValue(ctx, snapshotKey{s}, tx)); err != nil {
		return snapshot, err
	}
	return snapshot, tx.Commit()
}
