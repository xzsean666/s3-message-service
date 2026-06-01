package cursors

import (
	"testing"
	"time"
)

func TestEncodeDecodeCursor(t *testing.T) {
	window := time.Date(2026, 6, 1, 11, 22, 33, 0, time.UTC)
	cursor := Initial("mailbox:inbox", "actor-1", DirectionNewestFirst, window, 25)
	cursor.LastObjectKey = "mailboxes/actor-1/inbox/key.json"

	encoded, err := Encode(cursor)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Window.Second() != 0 {
		t.Fatalf("cursor window should be minute truncated: %s", decoded.Window)
	}
	if decoded.LastObjectKey != cursor.LastObjectKey {
		t.Fatalf("expected last key to round-trip")
	}
}

func TestNextWindow(t *testing.T) {
	window := time.Date(2026, 6, 1, 11, 22, 0, 0, time.UTC)
	if got := NextWindow(window, DirectionNewestFirst); !got.Equal(window.Add(-time.Minute)) {
		t.Fatalf("unexpected newest-first next window: %s", got)
	}
	if got := NextWindow(window, DirectionOldestFirst); !got.Equal(window.Add(time.Minute)) {
		t.Fatalf("unexpected oldest-first next window: %s", got)
	}
}
