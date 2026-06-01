package keys

import (
	"strings"
	"testing"
	"time"
)

func TestNormalizeExternalID(t *testing.T) {
	normalized := NormalizeExternalID(" User/ABC 01 ")
	if normalized != "user-abc-01" {
		t.Fatalf("expected normalized id, got %q", normalized)
	}
	if strings.Contains(NormalizeExternalID("a/b"), "/") {
		t.Fatal("normalized id must not contain slash")
	}
}

func TestTimePrefixAndSortKeys(t *testing.T) {
	older := time.Date(2026, 6, 1, 11, 22, 1, 0, time.UTC)
	newer := older.Add(time.Minute)

	if got := TimePrefix(older); got != "year=2026/month=06/day=01/hour=11/minute=22" {
		t.Fatalf("unexpected time prefix: %s", got)
	}
	if !(FeedSortKey(newer) < FeedSortKey(older)) {
		t.Fatal("newer feed sort key should sort before older feed sort key")
	}
	if !(ThreadSortKey(older) < ThreadSortKey(newer)) {
		t.Fatal("older thread sort key should sort before newer thread sort key")
	}
}

func TestBuilderAppliesNamespace(t *testing.T) {
	builder := NewBuilder("dev/test")
	key := builder.MessageLookup("message-1")
	if key != "dev-test/messages/by-id/message-1.json" {
		t.Fatalf("unexpected namespaced key: %s", key)
	}
}
