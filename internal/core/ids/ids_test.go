package ids

import (
	"crypto/rand"
	"strings"
	"testing"
	"time"
)

func TestGeneratorCreatesUUIDv7(t *testing.T) {
	generator := NewGenerator()
	identifier, err := generator.New()
	if err != nil {
		t.Fatal(err)
	}
	if len(identifier) != 36 {
		t.Fatalf("expected UUID length, got %q", identifier)
	}
	if identifier[14] != '7' {
		t.Fatalf("expected UUIDv7 version nibble, got %q", identifier)
	}
	variant := identifier[19]
	if !strings.ContainsRune("89ab", rune(variant)) {
		t.Fatalf("expected RFC variant nibble, got %q", variant)
	}
	if !IsSafeIdentifier(identifier) {
		t.Fatalf("identifier should be object-key safe: %s", identifier)
	}
}

func TestIsSafeIdentifierRejectsSlash(t *testing.T) {
	if IsSafeIdentifier("abc/def") {
		t.Fatal("slash should not be accepted in identifiers")
	}
}

func BenchmarkGenerator(b *testing.B) {
	generator := NewGeneratorForTest(rand.Reader, func() time.Time {
		return time.Now()
	})
	for i := 0; i < b.N; i++ {
		if _, err := generator.New(); err != nil {
			b.Fatal(err)
		}
	}
}
