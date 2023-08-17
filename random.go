package gorabbitmq

import (
	"crypto/rand"
	"encoding/hex"
)

const (
	defaultLength = 16
)

func newRandomString() string {
	bytes := make([]byte, defaultLength)

	_, err := rand.Read(bytes)
	if err != nil {
		return ""
	}

	return hex.EncodeToString(bytes)
}
