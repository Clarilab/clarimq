package gorabbitmq

import (
	"time"
)

type (
	PublishOption func(*PublishOptions)

	// PublishOptions are used to control how data is published.
	PublishOptions struct {
		// Application or exchange specific fields,
		// the headers exchange will inspect this field.
		Headers Table
		// Message timestamp.
		Timestamp time.Time
		// Exchange name.
		Exchange string
		// MIME content type.
		ContentType string
		// Expiration time in ms that a message will expire from a queue.
		// See https://www.rabbitmq.com/ttl.html#per-message-ttl-in-publishers
		Expiration string
		// MIME content encoding.
		ContentEncoding string
		// Correlation identifier.
		CorrelationID string
		// Address to reply to (ex: RPC).
		ReplyTo string
		// Message identifier.
		MessageID string
		// Message type name.
		Type string
		// Creating user id - default: "guest".
		UserID string
		// creating application id.
		AppID string
		// Mandatory fails to publish if there are no queues
		// bound to the routing key.
		Mandatory bool
		// Message priority level from 1 to 5 (0 == no priority).
		Priority Priority
		// Transient (0 or 1) or Persistent (2).
		DeliveryMode DeliveryMode
	}
)

func defaultPublishOptions() *PublishOptions {
	return &PublishOptions{
		Headers:         make(Table),
		Exchange:        "",
		ContentType:     "",
		Expiration:      "",
		ContentEncoding: "",
		CorrelationID:   "",
		ReplyTo:         "",
		MessageID:       "",
		Type:            "",
		UserID:          "",
		AppID:           "",
		Mandatory:       false,
		Priority:        NoPriority,
		DeliveryMode:    TransientDelivery,
	}
}

// WithCustomPublishOptions sets the publish options.
//
// It can be used to set all publisher options at once.
func WithCustomPublishOptions(options *PublishOptions) PublishOption {
	return func(opt *PublishOptions) {
		if options != nil {
			opt.AppID = options.AppID
			opt.ContentEncoding = options.ContentEncoding
			opt.ContentType = options.ContentType
			opt.CorrelationID = options.CorrelationID
			opt.DeliveryMode = options.DeliveryMode
			opt.Exchange = options.Exchange
			opt.Expiration = options.Expiration
			opt.Mandatory = options.Mandatory
			opt.MessageID = options.MessageID
			opt.Priority = options.Priority
			opt.ReplyTo = options.ReplyTo
			opt.Timestamp = options.Timestamp
			opt.Type = options.Type
			opt.UserID = options.UserID

			if options.Headers != nil {
				opt.Headers = options.Headers
			}
		}
	}
}

// WithPublishOptionExchange sets the exchange to publish to.
func WithPublishOptionExchange(exchange string) PublishOption {
	return func(options *PublishOptions) { options.Exchange = exchange }
}

// WithPublishOptionMandatory sets whether the publishing is mandatory, which means when a queue is not
// bound to the routing key a message will be sent back on the returns channel for you to handle.
//
// Default: false.
func WithPublishOptionMandatory(mandatory bool) PublishOption {
	return func(options *PublishOptions) { options.Mandatory = mandatory }
}

// WithPublishOptionContentType sets the content type, i.e. "application/json".
func WithPublishOptionContentType(contentType string) PublishOption {
	return func(options *PublishOptions) { options.ContentType = contentType }
}

// WithPublishOptionPersistentDelivery sets the message to persist. Transient messages will
// not be restored to durable queues, persistent messages will be restored to
// durable queues and lost on non-durable queues during server restart. By default publishings
// are transient.
func WithPublishOptionPersistentDelivery(deliveryMode DeliveryMode) PublishOption {
	return func(options *PublishOptions) { options.DeliveryMode = deliveryMode }
}

// WithPublishOptionExpiration sets the expiry/TTL of a message. As per RabbitMq spec, it must be a.
// string value in milliseconds.
func WithPublishOptionExpiration(expiration string) PublishOption {
	return func(options *PublishOptions) { options.Expiration = expiration }
}

// WithPublishOptionHeaders sets message header values, i.e. "msg-id".
func WithPublishOptionHeaders(headers Table) PublishOption {
	return func(options *PublishOptions) { options.Headers = headers }
}

// WithPublishOptionContentEncoding sets the content encoding, i.e. "utf-8".
func WithPublishOptionContentEncoding(contentEncoding string) PublishOption {
	return func(options *PublishOptions) { options.ContentEncoding = contentEncoding }
}

// WithPublishOptionPriority sets the content priority from 0 to 9.
func WithPublishOptionPriority(priority Priority) PublishOption {
	return func(options *PublishOptions) { options.Priority = priority }
}

// WithPublishOptionTracing sets the content correlation identifier.
func WithPublishOptionTracing(correlationID string) PublishOption {
	return func(options *PublishOptions) { options.CorrelationID = correlationID }
}

// WithPublishOptionReplyTo sets the reply to field.
func WithPublishOptionReplyTo(replyTo string) PublishOption {
	return func(options *PublishOptions) { options.ReplyTo = replyTo }
}

// WithPublishOptionMessageID sets the message identifier.
func WithPublishOptionMessageID(messageID string) PublishOption {
	return func(options *PublishOptions) { options.MessageID = messageID }
}

// WithPublishOptionTimestamp sets the timestamp for the message.
func WithPublishOptionTimestamp(timestamp time.Time) PublishOption {
	return func(options *PublishOptions) { options.Timestamp = timestamp }
}

// WithPublishOptionType sets the message type name.
func WithPublishOptionType(messageType string) PublishOption {
	return func(options *PublishOptions) { options.Type = messageType }
}

// WithPublishOptionUserID sets the user id e.g. "user".
func WithPublishOptionUserID(userID string) PublishOption {
	return func(options *PublishOptions) { options.UserID = userID }
}

// WithPublishOptionAppID sets the application id.
func WithPublishOptionAppID(appID string) PublishOption {
	return func(options *PublishOptions) { options.AppID = appID }
}
