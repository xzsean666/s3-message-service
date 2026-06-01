package ids

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"strings"
	"time"
)

type Generator struct {
	reader io.Reader
	clock  func() time.Time
}

func NewGenerator() Generator {
	return Generator{
		reader: rand.Reader,
		clock:  time.Now,
	}
}

func NewGeneratorForTest(reader io.Reader, clock func() time.Time) Generator {
	return Generator{
		reader: reader,
		clock:  clock,
	}
}

func (generator Generator) New() (string, error) {
	var uuid [16]byte
	now := generator.clock().UTC()
	timestampMillis := uint64(now.UnixNano() / int64(time.Millisecond))

	uuid[0] = byte(timestampMillis >> 40)
	uuid[1] = byte(timestampMillis >> 32)
	uuid[2] = byte(timestampMillis >> 24)
	uuid[3] = byte(timestampMillis >> 16)
	uuid[4] = byte(timestampMillis >> 8)
	uuid[5] = byte(timestampMillis)

	randomBytes := make([]byte, 10)
	if _, err := io.ReadFull(generator.reader, randomBytes); err != nil {
		return "", err
	}

	uuid[6] = 0x70 | (randomBytes[0] & 0x0f)
	uuid[7] = randomBytes[1]
	uuid[8] = 0x80 | (randomBytes[2] & 0x3f)
	copy(uuid[9:], randomBytes[3:])

	var encoded [36]byte
	hex.Encode(encoded[0:8], uuid[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], uuid[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], uuid[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], uuid[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], uuid[10:16])
	return string(encoded[:]), nil
}

func IsSafeIdentifier(identifier string) bool {
	if len(identifier) == 0 || len(identifier) > 128 {
		return false
	}
	for _, character := range identifier {
		switch {
		case character >= 'a' && character <= 'z':
		case character >= 'A' && character <= 'Z':
		case character >= '0' && character <= '9':
		case strings.ContainsRune("-_.:", character):
		default:
			return false
		}
	}
	return true
}
