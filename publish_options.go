package clarimq

import (
	"time"
)

type (
	// PublishingCache is an interface for a cache of messages that
	// could not be published due to a missing broker connection.
	PublishingCache interface {
		// Put adds a publishing to the cache.
		Put(Publishing) error
		// PopAll gets all publishing's from the cache and removes them.
		PopAll() ([]Publishing, error)
		// Len returns the number of publishing in the cache.
		Len() int
		// Flush removes all publishing's from the cache.
		Flush() error
	}

	// PublisherOptions are the options for a publisher.
	PublisherOptions struct {
		// PublishingCache is the publishing cache.
		PublishingCache PublishingCache
		// PublishingOptions are the options for publishing messages.
		PublishingOptions *PublishOptions
	}

	// PublisherOption is an option for a Publisher.
	PublisherOption func(*PublisherOptions)

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

func defaultPublisherOptions() *PublisherOptions {
	return &PublisherOptions{
		PublishingCache:   nil,
		PublishingOptions: defaultPublishOptions(),
	}
}

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
func WithCustomPublishOptions(options *PublisherOptions) PublisherOption {
	return func(opt *PublisherOptions) {
		if options != nil {
			if options.PublishingOptions != nil {
				opt.PublishingOptions.AppID = options.PublishingOptions.AppID
				opt.PublishingOptions.ContentEncoding = options.PublishingOptions.ContentEncoding
				opt.PublishingOptions.ContentType = options.PublishingOptions.ContentType
				opt.PublishingOptions.CorrelationID = options.PublishingOptions.CorrelationID
				opt.PublishingOptions.DeliveryMode = options.PublishingOptions.DeliveryMode
				opt.PublishingOptions.Exchange = options.PublishingOptions.Exchange
				opt.PublishingOptions.Expiration = options.PublishingOptions.Expiration
				opt.PublishingOptions.Mandatory = options.PublishingOptions.Mandatory
				opt.PublishingOptions.MessageID = options.PublishingOptions.MessageID
				opt.PublishingOptions.Priority = options.PublishingOptions.Priority
				opt.PublishingOptions.ReplyTo = options.PublishingOptions.ReplyTo
				opt.PublishingOptions.Timestamp = options.PublishingOptions.Timestamp
				opt.PublishingOptions.Type = options.PublishingOptions.Type
				opt.PublishingOptions.UserID = options.PublishingOptions.UserID

				if options.PublishingOptions.Headers != nil {
					opt.PublishingOptions.Headers = options.PublishingOptions.Headers
				}
			}

			if options.PublishingCache != nil {
				opt.PublishingCache = options.PublishingCache
			}
		}
	}
}

// WithPublishOptionExchange sets the exchange to publish to.
func WithPublishOptionExchange(exchange string) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.Exchange = exchange }
}

// WithPublishOptionMandatory sets whether the publishing is mandatory, which means when a queue is not
// bound to the routing key a message will be sent back on the returns channel for you to handle.
//
// Default: false.
func WithPublishOptionMandatory(mandatory bool) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.Mandatory = mandatory }
}

// WithPublishOptionContentType sets the content type, i.e. "application/json".
func WithPublishOptionContentType(contentType string) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.ContentType = contentType }
}

// WithPublishOptionDeliveryMode sets the message delivery mode. Transient messages will
// not be restored to durable queues, persistent messages will be restored to
// durable queues and lost on non-durable queues during broker restart. By default publishing's
// are transient.
func WithPublishOptionDeliveryMode(deliveryMode DeliveryMode) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.DeliveryMode = deliveryMode }
}

// WithPublishOptionExpiration sets the expiry/TTL of a message. As per RabbitMq spec, it must be a.
// string value in milliseconds.
func WithPublishOptionExpiration(expiration string) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.Expiration = expiration }
}

// WithPublishOptionHeaders sets message header values, i.e. "msg-id".
func WithPublishOptionHeaders(headers Table) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.Headers = headers }
}

// WithPublishOptionContentEncoding sets the content encoding, i.e. "utf-8".
func WithPublishOptionContentEncoding(contentEncoding string) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.ContentEncoding = contentEncoding }
}

// WithPublishOptionPriority sets the content priority from 0 to 9.
func WithPublishOptionPriority(priority Priority) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.Priority = priority }
}

// WithPublishOptionTracing sets the content correlation identifier.
func WithPublishOptionTracing(correlationID string) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.CorrelationID = correlationID }
}

// WithPublishOptionReplyTo sets the reply to field.
func WithPublishOptionReplyTo(replyTo string) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.ReplyTo = replyTo }
}

// WithPublishOptionMessageID sets the message identifier.
func WithPublishOptionMessageID(messageID string) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.MessageID = messageID }
}

// WithPublishOptionTimestamp sets the timestamp for the message.
func WithPublishOptionTimestamp(timestamp time.Time) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.Timestamp = timestamp }
}

// WithPublishOptionType sets the message type name.
func WithPublishOptionType(messageType string) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.Type = messageType }
}

// WithPublishOptionUserID sets the user id e.g. "user".
func WithPublishOptionUserID(userID string) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.UserID = userID }
}

// WithPublishOptionAppID sets the application id.
func WithPublishOptionAppID(appID string) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingOptions.AppID = appID }
}

// WithPublisherOptionPublishingCache enables the publishing cache.
//
// An implementation of the PublishingCache interface must be provided.
func WithPublisherOptionPublishingCache(cache PublishingCache) PublisherOption {
	return func(options *PublisherOptions) { options.PublishingCache = cache }
}
