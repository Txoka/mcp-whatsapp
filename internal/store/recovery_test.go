package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestCapturedBatchRecoversAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	chat := "recovery@g.us"
	if _, err = s.db.Exec("CREATE TRIGGER fail_test BEFORE INSERT ON messages WHEN NEW.id='blocked' BEGIN SELECT RAISE(ABORT,'injected storage failure'); END"); err != nil {
		t.Fatal(err)
	}
	first := Message{ID: "first", ChatJID: chat, Content: "first", Timestamp: time.Now()}
	second := first
	second.ID = "blocked"
	_, err = s.ApplyMessages(ctx, []MessageEvent{{Message: first, Incoming: true}, {Message: second, Incoming: true}})
	var projection *ProjectionError
	if !errors.As(err, &projection) {
		t.Fatalf("missing captured failure: %v", err)
	}
	var count int
	s.db.QueryRow("SELECT COUNT(*) FROM messages WHERE chat_jid=?", chat).Scan(&count)
	if count != 0 {
		t.Fatal("partial batch committed")
	}
	if _, err = s.db.Exec("DROP TRIGGER fail_test"); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.ReplayPending(ctx, chat); err != nil {
		t.Fatal(err)
	}
	s.db.QueryRow("SELECT COUNT(*) FROM messages WHERE chat_jid=?", chat).Scan(&count)
	if count != 2 {
		t.Fatalf("lost captured messages: %d", count)
	}
	var size int
	s.db.QueryRow("SELECT length(payload) FROM projection_inbox WHERE id=?", projection.ID).Scan(&size)
	if size != 0 {
		t.Fatal("applied input payload not cleared")
	}
}
func TestReadSnapshotDoesNotMixConcurrentRevisions(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	m := Message{ID: "m", ChatJID: "snapshot@g.us", Content: "before", Timestamp: time.Now()}
	if err = s.StoreMessage(ctx, m, nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	edit := m
	edit.Content = "after"
	_, err = s.ReadSnapshot(ctx, func(ctx context.Context) error {
		before, err := s.GetMessageContext(ctx, m.ID, 0, 0, m.ChatJID)
		if err != nil {
			return err
		}
		done := make(chan error, 1)
		go func() {
			_, err := s.ApplyMessages(context.Background(), []MessageEvent{{Kind: "edit", Message: edit, RevisionMS: 1000}})
			done <- err
		}()
		if err = <-done; err != nil {
			return err
		}
		after, err := s.GetMessageContext(ctx, m.ID, 0, 0, m.ChatJID)
		if err != nil {
			return err
		}
		if before.Message.Content != after.Message.Content {
			t.Fatal("one read mixed projection revisions")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	latest, err := s.GetMessageContext(ctx, m.ID, 0, 0, m.ChatJID)
	if err != nil || latest.Message.Content != "after" {
		t.Fatal("concurrent edit did not commit")
	}
}
func TestInterruptedMutationRemainsUnknown(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	op := Operation{ID: "request", Kind: "revoke", Chat: "123@g.us", MessageID: "m"}
	if _, _, err = s.ReserveOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.BeginSubmission(ctx, op.ID); err != nil || !ok {
		t.Fatal(err)
	}
	if err = s.RecoverInterrupted(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := s.Operation(ctx, op.ID)
	if err != nil || got.State != "unknown" {
		t.Fatalf("lost uncertainty: %+v %v", got, err)
	}
	if ok, err := s.BeginSubmission(ctx, op.ID); err != nil || ok {
		t.Fatal("uncertain mutation became resubmittable")
	}
	if _, err = s.ApplyMessages(ctx, []MessageEvent{{Kind: "revoke", Message: Message{ChatJID: op.Chat, ID: op.MessageID}}}); err != nil {
		t.Fatal(err)
	}
	got, err = s.Operation(ctx, op.ID)
	if err != nil || got.State != "applied" {
		t.Fatal("observed revoke failed reconciliation")
	}
}
func TestDeletedHighestUpdateDoesNotInvalidateCursor(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	m := Message{ID: "m", ChatJID: "cursor@g.us", Sender: "123", Content: "text", Timestamp: time.Now()}
	if _, err = s.ApplyMessages(ctx, []MessageEvent{{Message: m, Incoming: true}}); err != nil {
		t.Fatal(err)
	}
	head, err := s.WaitUpdates(ctx, "", "message", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteForMe(ctx, m.ChatJID, m.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.WaitUpdates(ctx, "", "message", head.Cursor, 0); err != nil {
		t.Fatal("delete invalidated durable cursor", err)
	}
}

func TestAcceptedOutgoingRecoversLocallyAfterRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	chat := "outbox@g.us"
	op := Operation{ID: "transport-id", Kind: "send", Chat: chat, MessageID: "transport-id"}
	if _, _, err = s.ReserveOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	event := MessageEvent{Message: Message{ID: op.ID, ChatJID: chat, Sender: "123@s.whatsapp.net", Content: "accepted text", IsFromMe: true}}
	if err = s.CaptureOutgoing(ctx, op.ID, chat, []MessageEvent{event}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.BeginSubmission(ctx, op.ID); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	if err = s.AcceptOutgoing(ctx, op.ID, at); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.RecoverInterrupted(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.ReplayOutgoing(ctx, chat); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetMessageContext(ctx, op.ID, 0, 0, chat)
	if err != nil || got.Message.Content != event.Message.Content || !got.Message.Timestamp.Equal(at) {
		t.Fatalf("accepted intent was lost: %+v %v", got, err)
	}
	var state string
	var size int
	s.db.QueryRow("SELECT state,length(payload) FROM outgoing_intents WHERE id=?", op.ID).Scan(&state, &size)
	if state != "complete" || size != 0 {
		t.Fatal("completed outgoing payload retained")
	}
}
func TestUnknownOutgoingIsNeverLocallyInvented(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	op := Operation{ID: "unknown-send", Kind: "send", Chat: "unknown@g.us", MessageID: "unknown-send"}
	s.ReserveOperation(ctx, op)
	s.CaptureOutgoing(ctx, op.ID, op.Chat, []MessageEvent{{Message: Message{ID: op.ID, ChatJID: op.Chat, Content: "not confirmed", IsFromMe: true}}})
	s.BeginSubmission(ctx, op.ID)
	if err = s.RecoverInterrupted(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.ReplayOutgoing(ctx, op.Chat); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetMessageContext(ctx, op.ID, 0, 0, op.Chat); err == nil {
		t.Fatal("uncertain send was projected as confirmed")
	}
}
