package cursors

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"
)

const Version = 1

var ErrInvalidCursor = errors.New("invalid cursor")

type Direction string

const (
	DirectionNewestFirst Direction = "newest-first"
	DirectionOldestFirst Direction = "oldest-first"
)

type Cursor struct {
	Version       int       `json:"version"`
	Kind          string    `json:"kind"`
	Owner         string    `json:"owner"`
	Direction     Direction `json:"direction"`
	Window        time.Time `json:"window"`
	LastObjectKey string    `json:"lastObjectKey,omitempty"`
	PageSize      int       `json:"pageSize"`
}

func Initial(kind string, owner string, direction Direction, window time.Time, pageSize int) Cursor {
	return Cursor{
		Version:   Version,
		Kind:      kind,
		Owner:     owner,
		Direction: direction,
		Window:    window.UTC().Truncate(time.Minute),
		PageSize:  pageSize,
	}
}

func Encode(cursor Cursor) (string, error) {
	cursor.Version = Version
	data, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func Decode(raw string) (Cursor, error) {
	if raw == "" {
		return Cursor{}, ErrInvalidCursor
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	var cursor Cursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	if cursor.Version != Version || cursor.Kind == "" || cursor.Owner == "" || cursor.PageSize <= 0 {
		return Cursor{}, ErrInvalidCursor
	}
	if cursor.Direction != DirectionNewestFirst && cursor.Direction != DirectionOldestFirst {
		return Cursor{}, ErrInvalidCursor
	}
	return cursor, nil
}

func NextWindow(window time.Time, direction Direction) time.Time {
	switch direction {
	case DirectionOldestFirst:
		return window.UTC().Truncate(time.Minute).Add(time.Minute)
	default:
		return window.UTC().Truncate(time.Minute).Add(-time.Minute)
	}
}
