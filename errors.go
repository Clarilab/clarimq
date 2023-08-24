package gorabbitmq

import (
	"errors"
)

// ErrNoActiveConnection occures when there is no active connection while trying to get the failed recovery notification channel.
var ErrNoActiveConnection = errors.New("no active connection to rabbitmq")

// ErrMaxRetriesExceeded occures when the maximum number of retries exceeds.
var ErrMaxRetriesExceeded = errors.New("max retries exceeded")

// ErrHealthyConnection occures if a manual reconnect is triggered but the connection persists.
var ErrHealthyConnection = errors.New("connection is healthy, no need to reconnect")
