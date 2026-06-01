package application

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sean/s3-message-service/internal/core/ids"
	"github.com/sean/s3-message-service/internal/core/keys"
	"github.com/sean/s3-message-service/internal/storage/localfs"
)

func TestServiceMessageMailboxThreadStateAttachmentAndBroadcast(t *testing.T) {
	ctx := context.Background()
	fixedNow := time.Date(2026, 6, 1, 11, 22, 33, 0, time.UTC)
	service := newTestService(t, fixedNow)

	sendResult, err := service.SendMessage(ctx, SendMessageCommand{
		CallerID:          "tests",
		IdempotencyKey:    "send-1",
		SenderActorID:     "actor-a",
		RecipientActorIDs: []string{"actor-b"},
		MessageType:       "text",
		Payload:           json.RawMessage(`{"text":"hello"}`),
		CreateThread:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sendResult.MessageID == "" || sendResult.MessageObjectKey == "" || sendResult.ThreadID == "" {
		t.Fatalf("unexpected send result: %+v", sendResult)
	}

	retryResult, err := service.SendMessage(ctx, SendMessageCommand{
		CallerID:          "tests",
		IdempotencyKey:    "send-1",
		SenderActorID:     "actor-a",
		RecipientActorIDs: []string{"actor-b"},
		MessageType:       "text",
		Payload:           json.RawMessage(`{"text":"hello"}`),
		CreateThread:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if retryResult.MessageID != sendResult.MessageID {
		t.Fatalf("idempotent retry should return same message id: %s != %s", retryResult.MessageID, sendResult.MessageID)
	}

	message, err := service.GetMessage(ctx, sendResult.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if message.SenderActorID != "actor-a" || message.ThreadID != sendResult.ThreadID {
		t.Fatalf("unexpected message: %+v", message)
	}

	mailbox, err := service.ListMailbox(ctx, "actor-b", "inbox", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(mailbox.Items) != 1 || mailbox.Items[0].Message.ID != sendResult.MessageID {
		t.Fatalf("unexpected mailbox result: %+v", mailbox)
	}

	markReadResult, err := service.MarkRead(ctx, MarkReadCommand{
		CallerID:       "tests",
		IdempotencyKey: "read-1",
		ActorID:        "actor-b",
		TargetKind:     "messages",
		TargetID:       sendResult.MessageID,
		ReadAt:         fixedNow.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if markReadResult.StateID == "" || markReadResult.CurrentStateObjectKey == "" {
		t.Fatalf("unexpected mark read result: %+v", markReadResult)
	}

	mailboxWithState, err := service.ListMailbox(ctx, "actor-b", "inbox", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if mailboxWithState.Items[0].ReadState == nil {
		t.Fatal("expected current read state in mailbox result")
	}

	thread, err := service.ListThread(ctx, sendResult.ThreadID, "actor-b", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(thread.Items) != 1 || thread.Items[0].Message.ID != sendResult.MessageID {
		t.Fatalf("unexpected thread result: %+v", thread)
	}

	attachment, err := service.CreateAttachment(ctx, CreateAttachmentCommand{
		CallerID:         "tests",
		IdempotencyKey:   "attachment-1",
		OriginalFileName: "Report Final.pdf",
		ContentType:      "application/pdf",
		Size:             1234,
		Checksum:         "sha256:test",
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := service.GetAttachment(ctx, attachment.AttachmentID)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.NormalizedFileName != "report-final.pdf" {
		t.Fatalf("unexpected attachment metadata: %+v", metadata)
	}

	broadcastResult, err := service.SendBroadcast(ctx, SendBroadcastCommand{
		CallerID:       "tests",
		IdempotencyKey: "broadcast-1",
		SenderActorID:  "system",
		AudienceType:   "tag",
		AudienceKeys:   []string{"beta"},
		MessageType:    "text",
		Payload:        json.RawMessage(`{"text":"announcement"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	broadcast, err := service.GetBroadcast(ctx, broadcastResult.BroadcastID)
	if err != nil {
		t.Fatal(err)
	}
	if broadcast.AudienceType != "tag" || broadcast.AudienceKeys[0] != "beta" {
		t.Fatalf("unexpected broadcast: %+v", broadcast)
	}
}

func TestListMailboxUsesPrefixWindowCursor(t *testing.T) {
	ctx := context.Background()
	fixedNow := time.Date(2026, 6, 1, 11, 22, 33, 0, time.UTC)
	service := newTestService(t, fixedNow)

	for i := 0; i < 3; i++ {
		_, err := service.SendMessage(ctx, SendMessageCommand{
			SenderActorID:     "actor-a",
			RecipientActorIDs: []string{"actor-b"},
			MessageType:       "text",
			Payload:           json.RawMessage(`{"text":"page"}`),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	firstPage, err := service.ListMailbox(ctx, "actor-b", "inbox", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstPage.Items) != 2 || firstPage.NextCursor == "" {
		t.Fatalf("expected first page with cursor, got %+v", firstPage)
	}
	secondPage, err := service.ListMailbox(ctx, "actor-b", "inbox", firstPage.NextCursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondPage.Items) != 1 {
		t.Fatalf("expected second page with remaining item, got %+v", secondPage)
	}
}

func newTestService(t *testing.T, now time.Time) *Service {
	t.Helper()
	store, err := localfs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return NewService(Options{
		Store:               store,
		KeyBuilder:          keys.NewBuilder(""),
		IDGenerator:         ids.NewGenerator(),
		Clock:               func() time.Time { return now },
		MaxPageSize:         50,
		ReadLookbackMinutes: 120,
	})
}
