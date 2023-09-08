package clarimq

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// errNoActiveConnection occurs when there is no active connection while trying to get the failed recovery notification channel.
var errNoActiveConnection = errors.New("no active connection to rabbitmq")

// ErrMaxRetriesExceeded occurs when the maximum number of retries exceeds.
var ErrMaxRetriesExceeded = errors.New("max retries exceeded")

// ErrHealthyConnection occurs when a manual reconnect is triggered but the connection persists.
var ErrHealthyConnection = errors.New("connection is healthy, no need to reconnect")

// ErrInvalidConnection occurs when an invalid connection is passed to a publisher or a consumer.
var ErrInvalidConnection = errors.New("invalid connection")

type AMQPError amqp.Error

func (e *AMQPError) Error() string {
	return fmt.Sprintf("Exception (%d) Reason: %q", e.Code, e.Reason)
}

// ErrRecoveryFailed occurs when the recovery failed after a connection loss.
type RecoveryFailedError struct {
	Err error
}

// Error implements the Error method of the error interface.
func (e *RecoveryFailedError) Error() string {
	var str string

	if e.Err != nil {
		str = e.Err.Error()
	} else {
		str = "unknown error"
	}

	return str
}
