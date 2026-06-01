package keys

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
	"unicode"
)

const maxTimestampMillis int64 = 9999999999999999

type Builder struct {
	namespace string
}

func NewBuilder(namespace string) Builder {
	return Builder{namespace: normalizeNamespace(namespace)}
}

func (builder Builder) MessageBody(messageID string, createdAt time.Time) string {
	return builder.withNamespace(fmt.Sprintf("messages/%s/%s.json", TimePrefix(createdAt), messageID))
}

func (builder Builder) MessageLookup(messageID string) string {
	return builder.withNamespace(fmt.Sprintf("messages/by-id/%s.json", messageID))
}

func (builder Builder) AttachmentMetadata(attachmentID string, createdAt time.Time) string {
	return builder.withNamespace(fmt.Sprintf("attachments/metadata/%s/%s.json", TimePrefix(createdAt), attachmentID))
}

func (builder Builder) AttachmentLookup(attachmentID string) string {
	return builder.withNamespace(fmt.Sprintf("attachments/by-id/%s.json", attachmentID))
}

func (builder Builder) AttachmentObject(attachmentID string, createdAt time.Time, normalizedFileName string) string {
	return builder.withNamespace(fmt.Sprintf("attachments/objects/%s/%s/%s", TimePrefix(createdAt), attachmentID, NormalizeExternalID(normalizedFileName)))
}

func (builder Builder) MailboxReference(actorID string, direction string, createdAt time.Time, messageID string, referenceID string) string {
	return builder.withNamespace(fmt.Sprintf(
		"mailboxes/%s/%s/%s/%s_%s_%s_%s.json",
		NormalizeExternalID(actorID),
		NormalizeExternalID(direction),
		TimePrefix(createdAt),
		FeedSortKey(createdAt),
		CompactTimestamp(createdAt),
		messageID,
		referenceID,
	))
}

func (builder Builder) MailboxPrefix(actorID string, direction string, window time.Time) string {
	return builder.withNamespace(fmt.Sprintf(
		"mailboxes/%s/%s/%s/",
		NormalizeExternalID(actorID),
		NormalizeExternalID(direction),
		TimePrefix(window),
	))
}

func (builder Builder) ThreadMetadata(threadID string) string {
	return builder.withNamespace(fmt.Sprintf("threads/%s/metadata.json", threadID))
}

func (builder Builder) ThreadReference(threadID string, createdAt time.Time, messageID string, referenceID string) string {
	return builder.withNamespace(fmt.Sprintf(
		"threads/%s/messages/%s/%s_%s_%s_%s.json",
		threadID,
		TimePrefix(createdAt),
		ThreadSortKey(createdAt),
		CompactTimestamp(createdAt),
		messageID,
		referenceID,
	))
}

func (builder Builder) ThreadPrefix(threadID string, window time.Time) string {
	return builder.withNamespace(fmt.Sprintf("threads/%s/messages/%s/", threadID, TimePrefix(window)))
}

func (builder Builder) BroadcastBody(broadcastID string, createdAt time.Time) string {
	return builder.withNamespace(fmt.Sprintf("broadcast/messages/%s/%s.json", TimePrefix(createdAt), broadcastID))
}

func (builder Builder) BroadcastLookup(broadcastID string) string {
	return builder.withNamespace(fmt.Sprintf("broadcast/by-id/%s.json", broadcastID))
}

func (builder Builder) BroadcastAudience(audienceType string, audienceKey string, createdAt time.Time, broadcastID string) string {
	return builder.withNamespace(fmt.Sprintf(
		"broadcast/audiences/%s/%s/%s/%s_%s.json",
		NormalizeExternalID(audienceType),
		NormalizeExternalID(audienceKey),
		TimePrefix(createdAt),
		FeedSortKey(createdAt),
		broadcastID,
	))
}

func (builder Builder) BroadcastAudiencePrefix(audienceType string, audienceKey string, window time.Time) string {
	return builder.withNamespace(fmt.Sprintf(
		"broadcast/audiences/%s/%s/%s/",
		NormalizeExternalID(audienceType),
		NormalizeExternalID(audienceKey),
		TimePrefix(window),
	))
}

func (builder Builder) StateEvent(actorID string, targetKind string, targetID string, createdAt time.Time, stateID string) string {
	return builder.withNamespace(fmt.Sprintf(
		"states/%s/events/%s/%s/%s/%s_%s.json",
		NormalizeExternalID(actorID),
		NormalizeExternalID(targetKind),
		NormalizeExternalID(targetID),
		TimePrefix(createdAt),
		FeedSortKey(createdAt),
		stateID,
	))
}

func (builder Builder) StateCurrent(actorID string, targetKind string, targetID string) string {
	return builder.withNamespace(fmt.Sprintf(
		"states/%s/current/%s/%s.json",
		NormalizeExternalID(actorID),
		NormalizeExternalID(targetKind),
		NormalizeExternalID(targetID),
	))
}

func (builder Builder) OperationID(idempotencyKey string, callerID string) string {
	return builder.withNamespace(fmt.Sprintf(
		"operations/idempotency/%s/%s.json",
		NormalizeExternalID(callerID),
		NormalizeExternalID(idempotencyKey),
	))
}

func (builder Builder) OperationStarted(operationID string) string {
	return builder.withNamespace(fmt.Sprintf("operations/by-id/%s/started.json", operationID))
}

func (builder Builder) OperationStep(operationID string, stepID string) string {
	return builder.withNamespace(fmt.Sprintf("operations/by-id/%s/steps/%s.json", operationID, NormalizeExternalID(stepID)))
}

func (builder Builder) OperationCompleted(operationID string) string {
	return builder.withNamespace(fmt.Sprintf("operations/by-id/%s/completed.json", operationID))
}

func (builder Builder) withNamespace(key string) string {
	if builder.namespace == "" {
		return key
	}
	return builder.namespace + "/" + strings.TrimLeft(key, "/")
}

func TimePrefix(instant time.Time) string {
	utc := instant.UTC()
	return fmt.Sprintf("year=%04d/month=%02d/day=%02d/hour=%02d/minute=%02d",
		utc.Year(), int(utc.Month()), utc.Day(), utc.Hour(), utc.Minute())
}

func CompactTimestamp(instant time.Time) string {
	return instant.UTC().Format("20060102T150405.000000000Z")
}

func FeedSortKey(instant time.Time) string {
	millis := instant.UTC().UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%016d", maxTimestampMillis-millis)
}

func ThreadSortKey(instant time.Time) string {
	millis := instant.UTC().UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%016d", millis)
}

func NormalizeExternalID(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "empty-" + shortHash(raw)
	}

	var builder strings.Builder
	previousDash := false
	for _, character := range strings.ToLower(trimmed) {
		valid := unicode.IsLetter(character) || unicode.IsDigit(character) || character == '-' || character == '_' || character == '.'
		if valid {
			builder.WriteRune(character)
			previousDash = false
			continue
		}
		if !previousDash {
			builder.WriteByte('-')
			previousDash = true
		}
	}

	normalized := strings.Trim(builder.String(), "-.")
	if normalized == "" {
		normalized = "id"
	}
	if len(normalized) > 96 {
		normalized = normalized[:72] + "-" + shortHash(trimmed)
	}
	return normalized
}

func normalizeNamespace(raw string) string {
	trimmed := strings.Trim(strings.TrimSpace(raw), "/")
	if trimmed == "" {
		return ""
	}
	return NormalizeExternalID(trimmed)
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:16]
}
