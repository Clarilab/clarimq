package clarimq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	jsonContentType   string = "application/json"
	stringContentType string = "text/plain"
	bytesContentType  string = "application/octet-stream"
)

// Publisher is a publisher for AMQP messages.
type Publisher struct {
	conn *Connection

	publisherOptions *PublisherOptions
	encoder          JSONEncoder
}

// Creates a new Publisher instance. Options can be passed to customize the behavior of the Publisher.
func NewPublisher(conn *Connection, options ...PublisherOption) (*Publisher, error) {
	const errMessage = "failed to create publisher: %w"

	if conn == nil {
		return nil, fmt.Errorf(errMessage, ErrInvalidConnection)
	}

	conn.isPublisher = true

	opt := defaultPublisherOptions()

	for i := range options {
		options[i](opt)
	}

	publisher := &Publisher{
		conn:             conn,
		publisherOptions: opt,
		encoder:          conn.options.codec.Encoder,
	}

	publisher.watchCheckPublishingCacheChan()

	return publisher, nil
}

// Close closes the Publisher.
//
// When using the publishing cache, the publisher must be closed
// to clear the cache.
func (publisher *Publisher) Close() error {
	const errMessage = "failed to close publisher: %w"

	if err := publisher.publisherOptions.PublishingCache.Flush(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

// Publish publishes a message with the publish options configured in the Publisher.
//
// target can be a queue name for direct publishing or a routing key.
func (publisher *Publisher) Publish(ctx context.Context, target string, data any) error {
	targets := []string{target}

	if publisher.conn.isClosed() {
		return publisher.cachePublishing(&publishing{
			PublishingID: newRandomString(),
			Targets:      targets,
			Data:         data,
			Options:      publisher.publisherOptions.PublishingOptions,
		})
	}

	return publisher.internalPublish(ctx, targets, data, publisher.publisherOptions.PublishingOptions)
}

// PublishWithOptions publishes a message to one or multiple targets.
//
// Targets can be a queue names for direct publishing or routing keys.
//
// Options can be passed to override the default options just for this publish.
func (publisher *Publisher) PublishWithOptions(ctx context.Context, targets []string, data any, options ...PublisherOption) error {
	// create new options to not override the default options
	opt := *publisher.publisherOptions

	for i := range options {
		options[i](&opt)
	}

	if publisher.conn.isClosed() {
		return publisher.cachePublishing(&publishing{
			PublishingID: newRandomString(),
			Targets:      targets,
			Data:         data,
			Options:      opt.PublishingOptions,
		})
	}

	return publisher.internalPublish(ctx, targets, data, opt.PublishingOptions)
}

func (publisher *Publisher) cachePublishing(publishing Publishing) error {
	const errMessage = "publishing failed: %w"

	if publisher.publisherOptions.PublishingCache != nil {
		if err := publisher.publisherOptions.PublishingCache.Put(publishing); err != nil {
			return fmt.Errorf(errMessage, err)
		}

		return fmt.Errorf(errMessage, ErrPublishFailedChannelClosedCached)
	}

	return fmt.Errorf(errMessage, ErrPublishFailedChannelClosed)
}

func (publisher *Publisher) internalPublish(ctx context.Context, routingKeys []string, data any, options *PublishOptions) error {
	const errMessage = "failed to publish: %w"

	body, err := publisher.encodeBody(data, options)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if err = publisher.sendMessage(ctx, routingKeys, body, options); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (publisher *Publisher) sendMessage(ctx context.Context, routingKeys []string, body []byte, options *PublishOptions) error {
	const errMessage = "failed to send message: %w"

	for _, key := range routingKeys {
		if options.MessageID == "" {
			options.MessageID = newRandomString()
		}

		if options.Timestamp.IsZero() {
			options.Timestamp = time.Now()
		}

		message := amqp.Publishing{
			Headers:         amqp.Table(options.Headers),
			Body:            body,
			DeliveryMode:    uint8(options.DeliveryMode),
			Priority:        uint8(options.Priority),
			ContentType:     options.ContentType,
			ContentEncoding: options.ContentEncoding,
			CorrelationId:   options.CorrelationID,
			ReplyTo:         options.ReplyTo,
			Expiration:      options.Expiration,
			MessageId:       options.MessageID,
			Timestamp:       options.Timestamp,
			Type:            options.Type,
			UserId:          options.UserID,
			AppId:           options.AppID,
		}

		if err := publisher.conn.amqpChannel.PublishWithContext(
			ctx,
			options.Exchange,
			key,
			options.Mandatory,
			false, // always set to false since RabbitMQ does not support immediate publishing
			message,
		); err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	return nil
}

func (publisher *Publisher) encodeBody(data any, options *PublishOptions) ([]byte, error) {
	const errMessage = "failed to encode body: %w"

	var body []byte

	switch content := data.(type) {
	case []byte:
		body = content

		if options.ContentType == "" {
			options.ContentType = bytesContentType
		}

	case string:
		body = []byte(content)

		if options.ContentType == "" {
			options.ContentType = stringContentType
		}

	default:
		var err error

		if body, err = publisher.encoder(data); err != nil {
			return nil, fmt.Errorf(errMessage, err)
		}

		if options.ContentType == "" {
			options.ContentType = jsonContentType
		}
	}

	return body, nil
}

func (publisher *Publisher) watchCheckPublishingCacheChan() {
	go func() {
		for range publisher.conn.checkPublishingCacheChan {
			if publisher.publisherOptions.PublishingCache != nil &&
				publisher.publisherOptions.PublishingCache.Len() > 0 {
				if err := publisher.PublishCachedMessages(context.Background()); err != nil {
					publisher.conn.errChanMU.Lock()
					publisher.conn.errChan <- err
					publisher.conn.errChanMU.Unlock()
				}
			}
		}
	}()
}

var ErrCacheNotSet = fmt.Errorf("publishing cache is not set")

func (publisher *Publisher) PublishCachedMessages(ctx context.Context) error {
	const errMessage = "failed to publish cached messages: %w"

	if publisher.publisherOptions.PublishingCache == nil {
		return fmt.Errorf(errMessage, ErrCacheNotSet)
	}

	cacheLen := publisher.publisherOptions.PublishingCache.Len()

	publishings, err := publisher.publisherOptions.PublishingCache.PopAll()
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if err = publisher.conn.amqpChannel.Confirm(false); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	for i := range publishings {
		if err = publisher.internalPublish(
			ctx,
			publishings[i].GetTargets(),
			publishings[i].GetData(),
			publishings[i].GetOptions(),
		); err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	publisher.conn.logger.logDebug("published messages from cache", "cachedMessagesPublished", cacheLen)

	return nil
}
