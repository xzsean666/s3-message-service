package domain

import (
	"encoding/json"
	"time"
)

const SchemaVersion = 1

type Message struct {
	SchemaVersion     int             `json:"schemaVersion"`
	ID                string          `json:"id"`
	SenderActorID     string          `json:"senderActorId"`
	RecipientActorIDs []string        `json:"recipientActorIds"`
	MessageType       string          `json:"messageType"`
	Payload           json.RawMessage `json:"payload"`
	AttachmentIDs     []string        `json:"attachmentIds,omitempty"`
	ThreadID          string          `json:"threadId,omitempty"`
	ParentMessageID   string          `json:"parentMessageId,omitempty"`
	CreatedAt         time.Time       `json:"createdAt"`
}

type LookupReference struct {
	SchemaVersion int       `json:"schemaVersion"`
	EntityKind    string    `json:"entityKind"`
	EntityID      string    `json:"entityId"`
	ObjectKey     string    `json:"objectKey"`
	CreatedAt     time.Time `json:"createdAt"`
}

type MessageReference struct {
	SchemaVersion    int       `json:"schemaVersion"`
	ID               string    `json:"id"`
	MessageID        string    `json:"messageId"`
	MessageObjectKey string    `json:"messageObjectKey"`
	OwnerID          string    `json:"ownerId"`
	ReferenceKind    string    `json:"referenceKind"`
	CreatedAt        time.Time `json:"createdAt"`
}

type Thread struct {
	SchemaVersion         int       `json:"schemaVersion"`
	ID                    string    `json:"id"`
	RootMessageID         string    `json:"rootMessageId,omitempty"`
	ExternalCorrelationID string    `json:"externalCorrelationId,omitempty"`
	CreatedAt             time.Time `json:"createdAt"`
}

type Broadcast struct {
	SchemaVersion int             `json:"schemaVersion"`
	ID            string          `json:"id"`
	SenderActorID string          `json:"senderActorId"`
	AudienceType  string          `json:"audienceType"`
	AudienceKeys  []string        `json:"audienceKeys"`
	MessageType   string          `json:"messageType"`
	Payload       json.RawMessage `json:"payload"`
	AttachmentIDs []string        `json:"attachmentIds,omitempty"`
	CreatedAt     time.Time       `json:"createdAt"`
	ExpiresAt     *time.Time      `json:"expiresAt,omitempty"`
}

type State struct {
	SchemaVersion int       `json:"schemaVersion"`
	ID            string    `json:"id"`
	ActorID       string    `json:"actorId"`
	StateKind     string    `json:"stateKind"`
	TargetKind    string    `json:"targetKind"`
	TargetID      string    `json:"targetId"`
	ReadPosition  string    `json:"readPosition,omitempty"`
	ReadAt        time.Time `json:"readAt"`
	CreatedAt     time.Time `json:"createdAt"`
}

type AttachmentMetadata struct {
	SchemaVersion      int       `json:"schemaVersion"`
	ID                 string    `json:"id"`
	ObjectKey          string    `json:"objectKey"`
	OriginalFileName   string    `json:"originalFileName"`
	NormalizedFileName string    `json:"normalizedFileName"`
	ContentType        string    `json:"contentType"`
	Size               int64     `json:"size"`
	Checksum           string    `json:"checksum,omitempty"`
	CreatedAt          time.Time `json:"createdAt"`
}

type OperationRecord struct {
	SchemaVersion  int               `json:"schemaVersion"`
	ID             string            `json:"id"`
	CallerID       string            `json:"callerId"`
	IdempotencyKey string            `json:"idempotencyKey"`
	Status         string            `json:"status"`
	EntityIDs      map[string]string `json:"entityIds,omitempty"`
	Result         json.RawMessage   `json:"result,omitempty"`
	CreatedAt      time.Time         `json:"createdAt"`
	UpdatedAt      time.Time         `json:"updatedAt"`
}

type OperationStep struct {
	SchemaVersion int       `json:"schemaVersion"`
	OperationID   string    `json:"operationId"`
	StepID        string    `json:"stepId"`
	ObjectKey     string    `json:"objectKey"`
	CreatedAt     time.Time `json:"createdAt"`
}
