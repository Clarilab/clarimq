package clarimq

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrNoActiveConnection occurs when there is no active connection while trying to get the failed recovery notification channel.
var ErrNoActiveConnection = errors.New("no active connection to broker")

// ErrPublishFailedChannelClosed occurs when the channel is accessed while being closed.
var ErrPublishFailedChannelClosed = errors.New("channel is closed")

// ErrPublishFailedChannelClosedCached occurs when the channel is accessed while being closed but publishing was cached.
var ErrPublishFailedChannelClosedCached = errors.New("channel is closed: publishing was cached")

// ErrMaxRetriesExceeded occurs when the maximum number of retries exceeds.
var ErrMaxRetriesExceeded = errors.New("max retries exceeded")

// ErrHealthyConnection occurs when a manual recovery is triggered but the connection persists.
var ErrHealthyConnection = errors.New("connection is healthy, no need to recover")

// ErrInvalidConnection occurs when an invalid connection is passed to a publisher or a consumer.
var ErrInvalidConnection = errors.New("invalid connection")

// ErrConsumerAlreadyRunning occurs when the consumer is attempted to be started but already running.
var ErrConsumerAlreadyRunning = errors.New("consumer is running")

// AMQPError is a custom error type that wraps amqp errors.
type AMQPError amqp.Error

func (e *AMQPError) Error() string {
	return fmt.Sprintf("Exception (%d) Reason: %q", e.Code, e.Reason)
}

// ErrRecoveryFailed occurs when the recovery failed after a connection loss.
type RecoveryFailedError struct {
	Err            error
	ConnectionName string
}

// Error implements the Error method of the error interface.
func (e *RecoveryFailedError) Error() string {
	str := fmt.Sprintf("failed to recover %s:", e.ConnectionName)

	if e.Err != nil {
		str += e.Err.Error()
	} else {
		str += "unknown error"
	}

	return str
}
