package clarimq

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	conn              *Connection
	channelExec       channelExec
	options           *PublisherOptions
	encoder           JSONEncoder
	publishingCacheMU *sync.Mutex
}

// Creates a new Publisher instance. Options can be passed to customize the behavior of the Publisher.
func NewPublisher(conn *Connection, options ...PublisherOption) (*Publisher, error) {
	const errMessage = "failed to create publisher: %w"

	if conn == nil {
		return nil, fmt.Errorf(errMessage, ErrInvalidConnection)
	}

	opt := defaultPublisherOptions()

	for i := range options {
		options[i](opt)
	}

	publisher := &Publisher{
		conn:        conn,
		channelExec: conn.channelExec,
		options:     opt,
		encoder:     conn.options.codec.Encoder,
	}

	if publisher.options.PublishingCache != nil {
		publisher.publishingCacheMU = new(sync.Mutex)
	}

	conn.addPublisherCheckCacheFN(publisher.options.PublisherName, publisher.checkPublishingCache)

	return publisher, nil
}

// Close closes the Publisher.
//
// When using the publishing cache, the publisher must be closed
// to clear the cache.
func (p *Publisher) Close() error {
	const errMessage = "failed to close publisher: %w"

	if p.options.PublishingCache != nil {
		p.publishingCacheMU.Lock()
		defer p.publishingCacheMU.Unlock()

		err := p.options.PublishingCache.Flush()
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	p.conn.removePublisherCheckCacheFN(p.options.PublisherName)

	return nil
}

// Publish publishes a message with the publish options configured in the Publisher.
//
// target can be a queue name for direct publishing or a routing key.
func (p *Publisher) Publish(ctx context.Context, target string, data any) error {
	targets := []string{target}

	if p.conn.isClosed() {
		return p.cachePublishing(&publishing{
			PublishingID: newRandomString(),
			Targets:      targets,
			Data:         data,
			Options:      p.options.PublishingOptions,
		})
	}

	return p.internalPublish(ctx, targets, data, p.options.PublishingOptions)
}

// PublishWithOptions publishes a message to one or multiple targets.
//
// Targets can be a queue names for direct publishing or routing keys.
//
// Options can be passed to override the default options just for this publish.
func (p *Publisher) PublishWithOptions(ctx context.Context, targets []string, data any, options ...PublisherOption) error {
	// create new options to not override the default options
	opt := *p.options

	for i := range options {
		options[i](&opt)
	}

	if p.conn.isClosed() {
		return p.cachePublishing(&publishing{
			PublishingID: newRandomString(),
			Targets:      targets,
			Data:         data,
			Options:      opt.PublishingOptions,
		})
	}

	return p.internalPublish(ctx, targets, data, opt.PublishingOptions)
}

func (p *Publisher) cachePublishing(publishing Publishing) error {
	const errMessage = "publishing failed: %w"

	if p.options.PublishingCache != nil {
		if err := p.options.PublishingCache.Put(publishing); err != nil {
			return fmt.Errorf(errMessage, err)
		}

		return fmt.Errorf(errMessage, ErrPublishFailedChannelClosedCached)
	}

	return fmt.Errorf(errMessage, ErrPublishFailedChannelClosed)
}

func (p *Publisher) internalPublish(ctx context.Context, routingKeys []string, data any, options *PublishOptions) error {
	const errMessage = "failed to publish: %w"

	body, err := p.encodeBody(data, options)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if err = p.sendMessage(ctx, routingKeys, body, options); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (p *Publisher) sendMessage(ctx context.Context, routingKeys []string, body []byte, options *PublishOptions) error {
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

		if err := p.channelExec(func(channel *amqp.Channel) error {
			return channel.PublishWithContext(
				ctx,
				options.Exchange,
				key,
				options.Mandatory,
				false, // always set to false since RabbitMQ does not support immediate publishing
				message,
			)
		}); err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	return nil
}

func (p *Publisher) encodeBody(data any, options *PublishOptions) ([]byte, error) {
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

		if body, err = p.encoder(data); err != nil {
			return nil, fmt.Errorf(errMessage, err)
		}

		if options.ContentType == "" {
			options.ContentType = jsonContentType
		}
	}

	return body, nil
}

func (p *Publisher) checkPublishingCache() {
	if p.options.PublishingCache != nil {
		p.publishingCacheMU.Lock()
		cacheLen := p.options.PublishingCache.Len()
		p.publishingCacheMU.Unlock()

		if cacheLen == 0 {
			return
		}

		if err := p.PublishCachedMessages(context.Background(), cacheLen); err != nil {
			p.conn.errChanMU.Lock()
			p.conn.errChan <- err
			p.conn.errChanMU.Unlock()
		}
	}
}

// ErrCacheNotSet occurs when the publishing cache is not set.
var ErrCacheNotSet = errors.New("publishing cache is not set")

func (p *Publisher) PublishCachedMessages(ctx context.Context, cacheLen int) error {
	const errMessage = "failed to publish cached messages: %w"

	if p.options.PublishingCache == nil {
		return fmt.Errorf(errMessage, ErrCacheNotSet)
	}

	p.publishingCacheMU.Lock()
	publishings, err := p.options.PublishingCache.PopAll()
	p.publishingCacheMU.Unlock()

	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	for i := range publishings {
		p.publishingCacheMU.Lock()

		targets := publishings[i].GetTargets()
		data := publishings[i].GetData()
		options := publishings[i].GetOptions()

		p.publishingCacheMU.Unlock()

		if err = p.internalPublish(
			ctx,
			targets,
			data,
			options,
		); err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	p.conn.logger.logDebug(context.Background(), "published messages from cache", "cachedMessagesPublished", cacheLen)

	return nil
}
