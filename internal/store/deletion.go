package store

import "context"

// DeleteForMe records a durable tombstone before removing the cached message.
func (s *Store) DeleteForMe(ctx context.Context, chat, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO deleted_messages(chat_jid,message_id) VALUES(?,?)", chat, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM messages WHERE chat_jid=? AND id=?", chat, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM message_mentions WHERE chat_jid=? AND message_id=?", chat, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM message_read_state WHERE chat_jid=? AND message_id=?", chat, id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM poll_votes WHERE poll_chat_jid=? AND poll_message_id=?", chat, id); err != nil {
		return err
	}
	return tx.Commit()
}
