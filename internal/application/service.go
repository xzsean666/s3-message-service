package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sean/s3-message-service/internal/core/cursors"
	"github.com/sean/s3-message-service/internal/core/ids"
	"github.com/sean/s3-message-service/internal/core/keys"
	"github.com/sean/s3-message-service/internal/domain"
	"github.com/sean/s3-message-service/internal/storage"
)

const defaultCallerID = "default"

var ErrValidation = errors.New("validation error")

type Clock func() time.Time

type Service struct {
	store               storage.ObjectStore
	keyBuilder          keys.Builder
	idGenerator         ids.Generator
	clock               Clock
	maxPageSize         int
	readLookbackMinutes int
}

type Options struct {
	Store               storage.ObjectStore
	KeyBuilder          keys.Builder
	IDGenerator         ids.Generator
	Clock               Clock
	MaxPageSize         int
	ReadLookbackMinutes int
}

func NewService(options Options) *Service {
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	maxPageSize := options.MaxPageSize
	if maxPageSize <= 0 {
		maxPageSize = 100
	}
	readLookbackMinutes := options.ReadLookbackMinutes
	if readLookbackMinutes <= 0 {
		readLookbackMinutes = 43200
	}
	return &Service{
		store:               options.Store,
		keyBuilder:          options.KeyBuilder,
		idGenerator:         options.IDGenerator,
		clock:               clock,
		maxPageSize:         maxPageSize,
		readLookbackMinutes: readLookbackMinutes,
	}
}

type SendMessageCommand struct {
	CallerID          string          `json:"callerId"`
	IdempotencyKey    string          `json:"idempotencyKey"`
	SenderActorID     string          `json:"senderActorId"`
	RecipientActorIDs []string        `json:"recipientActorIds"`
	MessageType       string          `json:"messageType"`
	Payload           json.RawMessage `json:"payload"`
	AttachmentIDs     []string        `json:"attachmentIds"`
	ThreadID          string          `json:"threadId"`
	ParentMessageID   string          `json:"parentMessageId"`
	CreateThread      bool            `json:"createThread"`
}

type SendMessageResult struct {
	OperationID      string   `json:"operationId,omitempty"`
	MessageID        string   `json:"messageId"`
	MessageObjectKey string   `json:"messageObjectKey"`
	MessageLookupKey string   `json:"messageLookupKey"`
	ThreadID         string   `json:"threadId,omitempty"`
	ReferenceKeys    []string `json:"referenceKeys"`
}

type MailboxItem struct {
	Reference domain.MessageReference `json:"reference"`
	Message   domain.Message          `json:"message"`
	ReadState *domain.State           `json:"readState,omitempty"`
}

type ListMailboxResult struct {
	Items      []MailboxItem `json:"items"`
	NextCursor string        `json:"nextCursor,omitempty"`
}

type ThreadItem struct {
	Reference domain.MessageReference `json:"reference"`
	Message   domain.Message          `json:"message"`
}

type ListThreadResult struct {
	Thread     domain.Thread `json:"thread"`
	Items      []ThreadItem  `json:"items"`
	NextCursor string        `json:"nextCursor,omitempty"`
	ReadState  *domain.State `json:"readState,omitempty"`
}

type SendBroadcastCommand struct {
	CallerID       string          `json:"callerId"`
	IdempotencyKey string          `json:"idempotencyKey"`
	SenderActorID  string          `json:"senderActorId"`
	AudienceType   string          `json:"audienceType"`
	AudienceKeys   []string        `json:"audienceKeys"`
	MessageType    string          `json:"messageType"`
	Payload        json.RawMessage `json:"payload"`
	AttachmentIDs  []string        `json:"attachmentIds"`
	ExpiresAt      *time.Time      `json:"expiresAt"`
}

type SendBroadcastResult struct {
	OperationID        string   `json:"operationId,omitempty"`
	BroadcastID        string   `json:"broadcastId"`
	BroadcastObjectKey string   `json:"broadcastObjectKey"`
	BroadcastLookupKey string   `json:"broadcastLookupKey"`
	AudienceObjectKeys []string `json:"audienceObjectKeys"`
}

type MarkReadCommand struct {
	CallerID       string    `json:"callerId"`
	IdempotencyKey string    `json:"idempotencyKey"`
	ActorID        string    `json:"actorId"`
	TargetKind     string    `json:"targetKind"`
	TargetID       string    `json:"targetId"`
	ReadPosition   string    `json:"readPosition"`
	ReadAt         time.Time `json:"readAt"`
}

type MarkReadResult struct {
	OperationID           string `json:"operationId,omitempty"`
	StateID               string `json:"stateId"`
	StateEventObjectKey   string `json:"stateEventObjectKey"`
	CurrentStateObjectKey string `json:"currentStateObjectKey"`
}

type CreateAttachmentCommand struct {
	CallerID         string `json:"callerId"`
	IdempotencyKey   string `json:"idempotencyKey"`
	ObjectKey        string `json:"objectKey"`
	OriginalFileName string `json:"originalFileName"`
	ContentType      string `json:"contentType"`
	Size             int64  `json:"size"`
	Checksum         string `json:"checksum"`
}

type CreateAttachmentResult struct {
	OperationID           string `json:"operationId,omitempty"`
	AttachmentID          string `json:"attachmentId"`
	AttachmentMetadataKey string `json:"attachmentMetadataKey"`
	AttachmentLookupKey   string `json:"attachmentLookupKey"`
	AttachmentObjectKey   string `json:"attachmentObjectKey"`
}

func (service *Service) SendMessage(ctx context.Context, command SendMessageCommand) (SendMessageResult, error) {
	if err := validateSendMessage(command); err != nil {
		return SendMessageResult{}, err
	}
	callerID := callerOrDefault(command.CallerID)
	createdAt := service.clock().UTC()
	entityIDs, err := service.messageEntityIDs(command)
	if err != nil {
		return SendMessageResult{}, err
	}
	operation, completed, err := service.beginOperation(ctx, callerID, command.IdempotencyKey, entityIDs, createdAt)
	if err != nil {
		return SendMessageResult{}, err
	}
	if completed {
		var result SendMessageResult
		if err := json.Unmarshal(operation.Result, &result); err != nil {
			return SendMessageResult{}, err
		}
		return result, nil
	}
	entityIDs = operation.EntityIDs
	createdAt = operation.CreatedAt

	messageID := entityIDs["messageId"]
	threadID := command.ThreadID
	if threadID == "" {
		threadID = entityIDs["threadId"]
	}
	messageObjectKey := service.keyBuilder.MessageBody(messageID, createdAt)
	message := domain.Message{
		SchemaVersion:     domain.SchemaVersion,
		ID:                messageID,
		SenderActorID:     command.SenderActorID,
		RecipientActorIDs: command.RecipientActorIDs,
		MessageType:       command.MessageType,
		Payload:           normalizedPayload(command.Payload),
		AttachmentIDs:     command.AttachmentIDs,
		ThreadID:          threadID,
		ParentMessageID:   command.ParentMessageID,
		CreatedAt:         createdAt,
	}
	if err := service.putJSON(ctx, messageObjectKey, message, true); err != nil && !errors.Is(err, storage.ErrObjectAlreadyExists) {
		return SendMessageResult{}, err
	}
	service.recordStep(ctx, operation.ID, "message-body", messageObjectKey)

	messageLookupKey := service.keyBuilder.MessageLookup(messageID)
	lookup := domain.LookupReference{
		SchemaVersion: domain.SchemaVersion,
		EntityKind:    "message",
		EntityID:      messageID,
		ObjectKey:     messageObjectKey,
		CreatedAt:     createdAt,
	}
	if err := service.putJSON(ctx, messageLookupKey, lookup, true); err != nil && !errors.Is(err, storage.ErrObjectAlreadyExists) {
		return SendMessageResult{}, err
	}
	service.recordStep(ctx, operation.ID, "message-lookup", messageLookupKey)

	referenceKeys := make([]string, 0, len(command.RecipientActorIDs)+2)
	sentReferenceID := entityIDs["sentRefId"]
	sentReferenceKey := service.keyBuilder.MailboxReference(command.SenderActorID, "sent", createdAt, messageID, sentReferenceID)
	sentReference := domain.MessageReference{
		SchemaVersion:    domain.SchemaVersion,
		ID:               sentReferenceID,
		MessageID:        messageID,
		MessageObjectKey: messageObjectKey,
		OwnerID:          command.SenderActorID,
		ReferenceKind:    "sent",
		CreatedAt:        createdAt,
	}
	if err := service.putJSON(ctx, sentReferenceKey, sentReference, true); err != nil && !errors.Is(err, storage.ErrObjectAlreadyExists) {
		return SendMessageResult{}, err
	}
	service.recordStep(ctx, operation.ID, "sent-reference", sentReferenceKey)
	referenceKeys = append(referenceKeys, sentReferenceKey)

	for index, recipientActorID := range command.RecipientActorIDs {
		referenceID := entityIDs[fmt.Sprintf("inboxRefId:%d", index)]
		referenceKey := service.keyBuilder.MailboxReference(recipientActorID, "inbox", createdAt, messageID, referenceID)
		reference := domain.MessageReference{
			SchemaVersion:    domain.SchemaVersion,
			ID:               referenceID,
			MessageID:        messageID,
			MessageObjectKey: messageObjectKey,
			OwnerID:          recipientActorID,
			ReferenceKind:    "inbox",
			CreatedAt:        createdAt,
		}
		if err := service.putJSON(ctx, referenceKey, reference, true); err != nil && !errors.Is(err, storage.ErrObjectAlreadyExists) {
			return SendMessageResult{}, err
		}
		service.recordStep(ctx, operation.ID, fmt.Sprintf("inbox-reference-%d", index), referenceKey)
		referenceKeys = append(referenceKeys, referenceKey)
	}

	if threadID != "" {
		threadMetadataKey := service.keyBuilder.ThreadMetadata(threadID)
		thread := domain.Thread{
			SchemaVersion: domain.SchemaVersion,
			ID:            threadID,
			RootMessageID: messageID,
			CreatedAt:     createdAt,
		}
		if err := service.putJSON(ctx, threadMetadataKey, thread, true); err != nil && !errors.Is(err, storage.ErrObjectAlreadyExists) {
			return SendMessageResult{}, err
		}
		service.recordStep(ctx, operation.ID, "thread-metadata", threadMetadataKey)

		threadReferenceID := entityIDs["threadRefId"]
		threadReferenceKey := service.keyBuilder.ThreadReference(threadID, createdAt, messageID, threadReferenceID)
		threadReference := domain.MessageReference{
			SchemaVersion:    domain.SchemaVersion,
			ID:               threadReferenceID,
			MessageID:        messageID,
			MessageObjectKey: messageObjectKey,
			OwnerID:          threadID,
			ReferenceKind:    "thread",
			CreatedAt:        createdAt,
		}
		if err := service.putJSON(ctx, threadReferenceKey, threadReference, true); err != nil && !errors.Is(err, storage.ErrObjectAlreadyExists) {
			return SendMessageResult{}, err
		}
		service.recordStep(ctx, operation.ID, "thread-reference", threadReferenceKey)
		referenceKeys = append(referenceKeys, threadReferenceKey)
	}

	result := SendMessageResult{
		OperationID:      operation.ID,
		MessageID:        messageID,
		MessageObjectKey: messageObjectKey,
		MessageLookupKey: messageLookupKey,
		ThreadID:         threadID,
		ReferenceKeys:    referenceKeys,
	}
	if err := service.completeOperation(ctx, callerID, command.IdempotencyKey, operation, result); err != nil {
		return SendMessageResult{}, err
	}
	return result, nil
}

func (service *Service) GetMessage(ctx context.Context, messageIDOrKey string) (domain.Message, error) {
	target := strings.TrimSpace(messageIDOrKey)
	if target == "" {
		return domain.Message{}, validationError("message id or key is required")
	}
	objectKey := target
	if !strings.HasPrefix(target, "messages/") && !strings.Contains(target, "/messages/") {
		var lookup domain.LookupReference
		if err := service.getJSON(ctx, service.keyBuilder.MessageLookup(target), &lookup); err != nil {
			return domain.Message{}, err
		}
		objectKey = lookup.ObjectKey
	}
	var message domain.Message
	if err := service.getJSON(ctx, objectKey, &message); err != nil {
		return domain.Message{}, err
	}
	return message, nil
}

func (service *Service) ListMailbox(ctx context.Context, actorID string, direction string, rawCursor string, limit int) (ListMailboxResult, error) {
	if strings.TrimSpace(actorID) == "" {
		return ListMailboxResult{}, validationError("actor id is required")
	}
	if direction != "inbox" && direction != "sent" {
		return ListMailboxResult{}, validationError("direction must be inbox or sent")
	}
	limit = service.normalizeLimit(limit)
	cursor, err := service.cursorOrInitial(rawCursor, "mailbox:"+direction, actorID, cursors.DirectionNewestFirst, service.clock(), limit)
	if err != nil {
		return ListMailboxResult{}, err
	}

	items := make([]MailboxItem, 0, limit)
	nextCursor, err := service.scanReferences(ctx, cursor, limit, func(window time.Time) string {
		return service.keyBuilder.MailboxPrefix(actorID, direction, window)
	}, func(reference domain.MessageReference) error {
		message, err := service.GetMessage(ctx, reference.MessageObjectKey)
		if err != nil {
			return err
		}
		state := service.getCurrentState(ctx, actorID, "messages", reference.MessageID)
		items = append(items, MailboxItem{
			Reference: reference,
			Message:   message,
			ReadState: state,
		})
		return nil
	})
	if err != nil {
		return ListMailboxResult{}, err
	}
	return ListMailboxResult{Items: items, NextCursor: nextCursor}, nil
}

func (service *Service) ListThread(ctx context.Context, threadID string, actorID string, rawCursor string, limit int) (ListThreadResult, error) {
	if strings.TrimSpace(threadID) == "" {
		return ListThreadResult{}, validationError("thread id is required")
	}
	var thread domain.Thread
	if err := service.getJSON(ctx, service.keyBuilder.ThreadMetadata(threadID), &thread); err != nil {
		return ListThreadResult{}, err
	}
	limit = service.normalizeLimit(limit)
	initialWindow := thread.CreatedAt
	cursor, err := service.cursorOrInitial(rawCursor, "thread", threadID, cursors.DirectionOldestFirst, initialWindow, limit)
	if err != nil {
		return ListThreadResult{}, err
	}
	items := make([]ThreadItem, 0, limit)
	nextCursor, err := service.scanReferences(ctx, cursor, limit, func(window time.Time) string {
		return service.keyBuilder.ThreadPrefix(threadID, window)
	}, func(reference domain.MessageReference) error {
		message, err := service.GetMessage(ctx, reference.MessageObjectKey)
		if err != nil {
			return err
		}
		items = append(items, ThreadItem{Reference: reference, Message: message})
		return nil
	})
	if err != nil {
		return ListThreadResult{}, err
	}
	var state *domain.State
	if actorID != "" {
		state = service.getCurrentState(ctx, actorID, "threads", threadID)
	}
	return ListThreadResult{
		Thread:     thread,
		Items:      items,
		NextCursor: nextCursor,
		ReadState:  state,
	}, nil
}

func (service *Service) SendBroadcast(ctx context.Context, command SendBroadcastCommand) (SendBroadcastResult, error) {
	if err := validateSendBroadcast(command); err != nil {
		return SendBroadcastResult{}, err
	}
	callerID := callerOrDefault(command.CallerID)
	createdAt := service.clock().UTC()
	entityIDs := map[string]string{}
	broadcastID, err := service.idGenerator.New()
	if err != nil {
		return SendBroadcastResult{}, err
	}
	entityIDs["broadcastId"] = broadcastID

	operation, completed, err := service.beginOperation(ctx, callerID, command.IdempotencyKey, entityIDs, createdAt)
	if err != nil {
		return SendBroadcastResult{}, err
	}
	if completed {
		var result SendBroadcastResult
		if err := json.Unmarshal(operation.Result, &result); err != nil {
			return SendBroadcastResult{}, err
		}
		return result, nil
	}
	broadcastID = operation.EntityIDs["broadcastId"]
	createdAt = operation.CreatedAt

	broadcastObjectKey := service.keyBuilder.BroadcastBody(broadcastID, createdAt)
	broadcast := domain.Broadcast{
		SchemaVersion: domain.SchemaVersion,
		ID:            broadcastID,
		SenderActorID: command.SenderActorID,
		AudienceType:  command.AudienceType,
		AudienceKeys:  normalizeAudienceKeys(command.AudienceType, command.AudienceKeys),
		MessageType:   command.MessageType,
		Payload:       normalizedPayload(command.Payload),
		AttachmentIDs: command.AttachmentIDs,
		CreatedAt:     createdAt,
		ExpiresAt:     command.ExpiresAt,
	}
	if err := service.putJSON(ctx, broadcastObjectKey, broadcast, true); err != nil && !errors.Is(err, storage.ErrObjectAlreadyExists) {
		return SendBroadcastResult{}, err
	}
	service.recordStep(ctx, operation.ID, "broadcast-body", broadcastObjectKey)

	broadcastLookupKey := service.keyBuilder.BroadcastLookup(broadcastID)
	lookup := domain.LookupReference{
		SchemaVersion: domain.SchemaVersion,
		EntityKind:    "broadcast",
		EntityID:      broadcastID,
		ObjectKey:     broadcastObjectKey,
		CreatedAt:     createdAt,
	}
	if err := service.putJSON(ctx, broadcastLookupKey, lookup, true); err != nil && !errors.Is(err, storage.ErrObjectAlreadyExists) {
		return SendBroadcastResult{}, err
	}
	service.recordStep(ctx, operation.ID, "broadcast-lookup", broadcastLookupKey)

	audienceObjectKeys := make([]string, 0, len(broadcast.AudienceKeys))
	for _, audienceKey := range broadcast.AudienceKeys {
		audienceObjectKey := service.keyBuilder.BroadcastAudience(command.AudienceType, audienceKey, createdAt, broadcastID)
		if err := service.putJSON(ctx, audienceObjectKey, lookup, true); err != nil && !errors.Is(err, storage.ErrObjectAlreadyExists) {
			return SendBroadcastResult{}, err
		}
		service.recordStep(ctx, operation.ID, "broadcast-audience-"+audienceKey, audienceObjectKey)
		audienceObjectKeys = append(audienceObjectKeys, audienceObjectKey)
	}

	result := SendBroadcastResult{
		OperationID:        operation.ID,
		BroadcastID:        broadcastID,
		BroadcastObjectKey: broadcastObjectKey,
		BroadcastLookupKey: broadcastLookupKey,
		AudienceObjectKeys: audienceObjectKeys,
	}
	if err := service.completeOperation(ctx, callerID, command.IdempotencyKey, operation, result); err != nil {
		return SendBroadcastResult{}, err
	}
	return result, nil
}

func (service *Service) GetBroadcast(ctx context.Context, broadcastIDOrKey string) (domain.Broadcast, error) {
	target := strings.TrimSpace(broadcastIDOrKey)
	if target == "" {
		return domain.Broadcast{}, validationError("broadcast id or key is required")
	}
	objectKey := target
	if !strings.HasPrefix(target, "broadcast/") && !strings.Contains(target, "/broadcast/") {
		var lookup domain.LookupReference
		if err := service.getJSON(ctx, service.keyBuilder.BroadcastLookup(target), &lookup); err != nil {
			return domain.Broadcast{}, err
		}
		objectKey = lookup.ObjectKey
	}
	var broadcast domain.Broadcast
	if err := service.getJSON(ctx, objectKey, &broadcast); err != nil {
		return domain.Broadcast{}, err
	}
	return broadcast, nil
}

func (service *Service) MarkRead(ctx context.Context, command MarkReadCommand) (MarkReadResult, error) {
	if err := validateMarkRead(command); err != nil {
		return MarkReadResult{}, err
	}
	callerID := callerOrDefault(command.CallerID)
	createdAt := service.clock().UTC()
	if command.ReadAt.IsZero() {
		command.ReadAt = createdAt
	}
	stateID, err := service.idGenerator.New()
	if err != nil {
		return MarkReadResult{}, err
	}
	entityIDs := map[string]string{"stateId": stateID}
	operation, completed, err := service.beginOperation(ctx, callerID, command.IdempotencyKey, entityIDs, createdAt)
	if err != nil {
		return MarkReadResult{}, err
	}
	if completed {
		var result MarkReadResult
		if err := json.Unmarshal(operation.Result, &result); err != nil {
			return MarkReadResult{}, err
		}
		return result, nil
	}
	stateID = operation.EntityIDs["stateId"]
	createdAt = operation.CreatedAt

	state := domain.State{
		SchemaVersion: domain.SchemaVersion,
		ID:            stateID,
		ActorID:       command.ActorID,
		StateKind:     "read",
		TargetKind:    command.TargetKind,
		TargetID:      command.TargetID,
		ReadPosition:  command.ReadPosition,
		ReadAt:        command.ReadAt.UTC(),
		CreatedAt:     createdAt,
	}
	stateEventKey := service.keyBuilder.StateEvent(command.ActorID, command.TargetKind, command.TargetID, createdAt, stateID)
	if err := service.putJSON(ctx, stateEventKey, state, true); err != nil && !errors.Is(err, storage.ErrObjectAlreadyExists) {
		return MarkReadResult{}, err
	}
	service.recordStep(ctx, operation.ID, "state-event", stateEventKey)

	currentKey := service.keyBuilder.StateCurrent(command.ActorID, command.TargetKind, command.TargetID)
	existing := service.getCurrentState(ctx, command.ActorID, command.TargetKind, command.TargetID)
	if existing == nil || !existing.ReadAt.After(state.ReadAt) {
		if err := service.putJSON(ctx, currentKey, state, false); err != nil {
			return MarkReadResult{}, err
		}
		service.recordStep(ctx, operation.ID, "state-current", currentKey)
	}

	result := MarkReadResult{
		OperationID:           operation.ID,
		StateID:               stateID,
		StateEventObjectKey:   stateEventKey,
		CurrentStateObjectKey: currentKey,
	}
	if err := service.completeOperation(ctx, callerID, command.IdempotencyKey, operation, result); err != nil {
		return MarkReadResult{}, err
	}
	return result, nil
}

func (service *Service) CreateAttachment(ctx context.Context, command CreateAttachmentCommand) (CreateAttachmentResult, error) {
	if err := validateCreateAttachment(command); err != nil {
		return CreateAttachmentResult{}, err
	}
	callerID := callerOrDefault(command.CallerID)
	createdAt := service.clock().UTC()
	attachmentID, err := service.idGenerator.New()
	if err != nil {
		return CreateAttachmentResult{}, err
	}
	entityIDs := map[string]string{"attachmentId": attachmentID}
	operation, completed, err := service.beginOperation(ctx, callerID, command.IdempotencyKey, entityIDs, createdAt)
	if err != nil {
		return CreateAttachmentResult{}, err
	}
	if completed {
		var result CreateAttachmentResult
		if err := json.Unmarshal(operation.Result, &result); err != nil {
			return CreateAttachmentResult{}, err
		}
		return result, nil
	}
	attachmentID = operation.EntityIDs["attachmentId"]
	createdAt = operation.CreatedAt

	normalizedFileName := keys.NormalizeExternalID(command.OriginalFileName)
	objectKey := strings.TrimSpace(command.ObjectKey)
	if objectKey == "" {
		objectKey = service.keyBuilder.AttachmentObject(attachmentID, createdAt, normalizedFileName)
	}
	metadata := domain.AttachmentMetadata{
		SchemaVersion:      domain.SchemaVersion,
		ID:                 attachmentID,
		ObjectKey:          objectKey,
		OriginalFileName:   command.OriginalFileName,
		NormalizedFileName: normalizedFileName,
		ContentType:        command.ContentType,
		Size:               command.Size,
		Checksum:           command.Checksum,
		CreatedAt:          createdAt,
	}
	metadataKey := service.keyBuilder.AttachmentMetadata(attachmentID, createdAt)
	if err := service.putJSON(ctx, metadataKey, metadata, true); err != nil && !errors.Is(err, storage.ErrObjectAlreadyExists) {
		return CreateAttachmentResult{}, err
	}
	service.recordStep(ctx, operation.ID, "attachment-metadata", metadataKey)

	lookupKey := service.keyBuilder.AttachmentLookup(attachmentID)
	lookup := domain.LookupReference{
		SchemaVersion: domain.SchemaVersion,
		EntityKind:    "attachment",
		EntityID:      attachmentID,
		ObjectKey:     metadataKey,
		CreatedAt:     createdAt,
	}
	if err := service.putJSON(ctx, lookupKey, lookup, true); err != nil && !errors.Is(err, storage.ErrObjectAlreadyExists) {
		return CreateAttachmentResult{}, err
	}
	service.recordStep(ctx, operation.ID, "attachment-lookup", lookupKey)

	result := CreateAttachmentResult{
		OperationID:           operation.ID,
		AttachmentID:          attachmentID,
		AttachmentMetadataKey: metadataKey,
		AttachmentLookupKey:   lookupKey,
		AttachmentObjectKey:   objectKey,
	}
	if err := service.completeOperation(ctx, callerID, command.IdempotencyKey, operation, result); err != nil {
		return CreateAttachmentResult{}, err
	}
	return result, nil
}

func (service *Service) GetAttachment(ctx context.Context, attachmentIDOrKey string) (domain.AttachmentMetadata, error) {
	target := strings.TrimSpace(attachmentIDOrKey)
	if target == "" {
		return domain.AttachmentMetadata{}, validationError("attachment id or key is required")
	}
	objectKey := target
	if !strings.HasPrefix(target, "attachments/") && !strings.Contains(target, "/attachments/") {
		var lookup domain.LookupReference
		if err := service.getJSON(ctx, service.keyBuilder.AttachmentLookup(target), &lookup); err != nil {
			return domain.AttachmentMetadata{}, err
		}
		objectKey = lookup.ObjectKey
	}
	var metadata domain.AttachmentMetadata
	if err := service.getJSON(ctx, objectKey, &metadata); err != nil {
		return domain.AttachmentMetadata{}, err
	}
	return metadata, nil
}

func (service *Service) messageEntityIDs(command SendMessageCommand) (map[string]string, error) {
	entityIDs := map[string]string{}
	messageID, err := service.idGenerator.New()
	if err != nil {
		return nil, err
	}
	entityIDs["messageId"] = messageID
	sentReferenceID, err := service.idGenerator.New()
	if err != nil {
		return nil, err
	}
	entityIDs["sentRefId"] = sentReferenceID
	for index := range command.RecipientActorIDs {
		referenceID, err := service.idGenerator.New()
		if err != nil {
			return nil, err
		}
		entityIDs[fmt.Sprintf("inboxRefId:%d", index)] = referenceID
	}
	if command.ThreadID == "" && (command.CreateThread || command.ParentMessageID != "") {
		threadID, err := service.idGenerator.New()
		if err != nil {
			return nil, err
		}
		entityIDs["threadId"] = threadID
	}
	if command.ThreadID != "" || command.CreateThread || command.ParentMessageID != "" {
		threadReferenceID, err := service.idGenerator.New()
		if err != nil {
			return nil, err
		}
		entityIDs["threadRefId"] = threadReferenceID
	}
	return entityIDs, nil
}

func (service *Service) beginOperation(ctx context.Context, callerID string, idempotencyKey string, entityIDs map[string]string, createdAt time.Time) (domain.OperationRecord, bool, error) {
	operationID, err := service.idGenerator.New()
	if err != nil {
		return domain.OperationRecord{}, false, err
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		operation := domain.OperationRecord{
			SchemaVersion: domain.SchemaVersion,
			ID:            operationID,
			CallerID:      callerID,
			Status:        "pending",
			EntityIDs:     entityIDs,
			CreatedAt:     createdAt,
			UpdatedAt:     createdAt,
		}
		_ = service.putJSON(ctx, service.keyBuilder.OperationStarted(operationID), operation, true)
		return operation, false, nil
	}

	operation := domain.OperationRecord{
		SchemaVersion:  domain.SchemaVersion,
		ID:             operationID,
		CallerID:       callerID,
		IdempotencyKey: idempotencyKey,
		Status:         "pending",
		EntityIDs:      entityIDs,
		CreatedAt:      createdAt,
		UpdatedAt:      createdAt,
	}
	idempotencyObjectKey := service.keyBuilder.OperationID(idempotencyKey, callerID)
	err = service.putJSON(ctx, idempotencyObjectKey, operation, true)
	if err == nil {
		_ = service.putJSON(ctx, service.keyBuilder.OperationStarted(operation.ID), operation, true)
		return operation, false, nil
	}
	if !errors.Is(err, storage.ErrObjectAlreadyExists) {
		return domain.OperationRecord{}, false, err
	}

	var existing domain.OperationRecord
	if err := service.getJSON(ctx, idempotencyObjectKey, &existing); err != nil {
		return domain.OperationRecord{}, false, err
	}
	if existing.Status == "completed" {
		return existing, true, nil
	}
	return existing, false, nil
}

func (service *Service) completeOperation(ctx context.Context, callerID string, idempotencyKey string, operation domain.OperationRecord, result any) error {
	resultBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}
	now := service.clock().UTC()
	completed := operation
	completed.Status = "completed"
	completed.Result = resultBytes
	completed.UpdatedAt = now

	completedKey := service.keyBuilder.OperationCompleted(operation.ID)
	if err := service.putJSON(ctx, completedKey, completed, false); err != nil {
		return err
	}
	if strings.TrimSpace(idempotencyKey) != "" {
		idempotencyObjectKey := service.keyBuilder.OperationID(idempotencyKey, callerID)
		if err := service.putJSON(ctx, idempotencyObjectKey, completed, false); err != nil {
			return err
		}
	}
	return nil
}

func (service *Service) recordStep(ctx context.Context, operationID string, stepID string, objectKey string) {
	if operationID == "" || objectKey == "" {
		return
	}
	step := domain.OperationStep{
		SchemaVersion: domain.SchemaVersion,
		OperationID:   operationID,
		StepID:        stepID,
		ObjectKey:     objectKey,
		CreatedAt:     service.clock().UTC(),
	}
	_ = service.putJSON(ctx, service.keyBuilder.OperationStep(operationID, stepID), step, false)
}

func (service *Service) scanReferences(ctx context.Context, cursor cursors.Cursor, limit int, prefixForWindow func(time.Time) string, consume func(domain.MessageReference) error) (string, error) {
	remainingWindows := service.readLookbackMinutes
	window := cursor.Window.UTC().Truncate(time.Minute)
	lastObjectKey := cursor.LastObjectKey
	var lastReadKey string
	var nextWindow time.Time
	nextLastKey := ""
	collected := 0

	for remainingWindows > 0 && collected < limit {
		prefix := prefixForWindow(window)
		page, err := service.store.List(ctx, storage.ListInput{
			Prefix:     prefix,
			StartAfter: lastObjectKey,
			Limit:      limit - collected,
		})
		if err != nil {
			return "", err
		}
		for _, object := range page.Objects {
			var reference domain.MessageReference
			if err := service.getJSON(ctx, object.Key, &reference); err != nil {
				return "", err
			}
			if err := consume(reference); err != nil {
				return "", err
			}
			collected++
			lastReadKey = object.Key
			if collected >= limit {
				break
			}
		}
		if collected >= limit {
			if page.HasMore {
				nextWindow = window
				nextLastKey = lastReadKey
			} else {
				nextWindow = cursors.NextWindow(window, cursor.Direction)
			}
			break
		}
		window = cursors.NextWindow(window, cursor.Direction)
		lastObjectKey = ""
		remainingWindows--
	}

	if collected == 0 && remainingWindows <= 0 {
		return "", nil
	}
	if collected < limit && remainingWindows <= 0 {
		return "", nil
	}
	if nextWindow.IsZero() {
		nextWindow = window
	}
	next := cursors.Cursor{
		Version:       cursors.Version,
		Kind:          cursor.Kind,
		Owner:         cursor.Owner,
		Direction:     cursor.Direction,
		Window:        nextWindow.UTC().Truncate(time.Minute),
		LastObjectKey: nextLastKey,
		PageSize:      limit,
	}
	return cursors.Encode(next)
}

func (service *Service) cursorOrInitial(rawCursor string, kind string, owner string, direction cursors.Direction, initialWindow time.Time, pageSize int) (cursors.Cursor, error) {
	if rawCursor == "" {
		return cursors.Initial(kind, owner, direction, initialWindow, pageSize), nil
	}
	cursor, err := cursors.Decode(rawCursor)
	if err != nil {
		return cursors.Cursor{}, err
	}
	if cursor.Kind != kind || cursor.Owner != owner {
		return cursors.Cursor{}, cursors.ErrInvalidCursor
	}
	return cursor, nil
}

func (service *Service) normalizeLimit(limit int) int {
	if limit <= 0 {
		return service.maxPageSize
	}
	if limit > service.maxPageSize {
		return service.maxPageSize
	}
	return limit
}

func (service *Service) getCurrentState(ctx context.Context, actorID string, targetKind string, targetID string) *domain.State {
	var state domain.State
	if err := service.getJSON(ctx, service.keyBuilder.StateCurrent(actorID, targetKind, targetID), &state); err != nil {
		return nil
	}
	return &state
}

func (service *Service) putJSON(ctx context.Context, objectKey string, value any, createOnly bool) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return service.store.Put(ctx, objectKey, append(data, '\n'), storage.PutOptions{
		CreateOnly:  createOnly,
		ContentType: "application/json",
	})
}

func (service *Service) getJSON(ctx context.Context, objectKey string, target any) error {
	data, err := service.store.Get(ctx, objectKey)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func validateSendMessage(command SendMessageCommand) error {
	if strings.TrimSpace(command.SenderActorID) == "" {
		return validationError("senderActorId is required")
	}
	if len(command.RecipientActorIDs) == 0 {
		return validationError("recipientActorIds must contain at least one actor")
	}
	for _, recipientActorID := range command.RecipientActorIDs {
		if strings.TrimSpace(recipientActorID) == "" {
			return validationError("recipientActorIds cannot contain empty actor ids")
		}
	}
	if strings.TrimSpace(command.MessageType) == "" {
		return validationError("messageType is required")
	}
	if !validJSONPayload(command.Payload) {
		return validationError("payload must be valid JSON")
	}
	return nil
}

func validateSendBroadcast(command SendBroadcastCommand) error {
	if strings.TrimSpace(command.SenderActorID) == "" {
		return validationError("senderActorId is required")
	}
	if command.AudienceType != "all" && command.AudienceType != "tag" && command.AudienceType != "explicit" {
		return validationError("audienceType must be all, tag, or explicit")
	}
	if command.AudienceType != "all" && len(command.AudienceKeys) == 0 {
		return validationError("audienceKeys are required for tag and explicit broadcasts")
	}
	if strings.TrimSpace(command.MessageType) == "" {
		return validationError("messageType is required")
	}
	if !validJSONPayload(command.Payload) {
		return validationError("payload must be valid JSON")
	}
	return nil
}

func validateMarkRead(command MarkReadCommand) error {
	if strings.TrimSpace(command.ActorID) == "" {
		return validationError("actorId is required")
	}
	if command.TargetKind != "messages" && command.TargetKind != "threads" {
		return validationError("targetKind must be messages or threads")
	}
	if strings.TrimSpace(command.TargetID) == "" {
		return validationError("targetId is required")
	}
	return nil
}

func validateCreateAttachment(command CreateAttachmentCommand) error {
	if strings.TrimSpace(command.OriginalFileName) == "" {
		return validationError("originalFileName is required")
	}
	if strings.TrimSpace(command.ContentType) == "" {
		return validationError("contentType is required")
	}
	if command.Size < 0 {
		return validationError("size cannot be negative")
	}
	return nil
}

func validationError(message string) error {
	return fmt.Errorf("%w: %s", ErrValidation, message)
}

func callerOrDefault(callerID string) string {
	trimmed := strings.TrimSpace(callerID)
	if trimmed == "" {
		return defaultCallerID
	}
	return trimmed
}

func normalizedPayload(payload json.RawMessage) json.RawMessage {
	if len(payload) == 0 {
		return json.RawMessage(`{}`)
	}
	return payload
}

func validJSONPayload(payload json.RawMessage) bool {
	if len(payload) == 0 {
		return true
	}
	return json.Valid(payload)
}

func normalizeAudienceKeys(audienceType string, audienceKeys []string) []string {
	if audienceType == "all" {
		return []string{"all"}
	}
	normalized := make([]string, 0, len(audienceKeys))
	seen := map[string]bool{}
	for _, audienceKey := range audienceKeys {
		trimmed := strings.TrimSpace(audienceKey)
		if trimmed == "" || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		normalized = append(normalized, trimmed)
	}
	return normalized
}
