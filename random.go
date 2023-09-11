package clarimq

import (
	"crypto/rand"
	"encoding/hex"
)

const (
	defaultLength = 16
)

func newRandomString() string {
	bytes := make([]byte, defaultLength)

	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}

	return hex.EncodeToString(bytes)
}
