package store

import "context"

func (s *Store) DeleteForMe(ctx context.Context, chat, id string) error {
	_, err := s.ApplyMessages(ctx, []MessageEvent{{Kind: "delete_me", Message: Message{ChatJID: chat, ID: id}}})
	return err
}
