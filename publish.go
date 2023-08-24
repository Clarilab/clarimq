package gorabbitmq

import (
	"context"
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
	channel *amqp.Channel
	options *PublishOptions
	encoder JSONEncoder
	name    string
}

// Creates a new Publisher instance. Options can be passed to customize the behavior of the Publisher.
func (c *Connector) NewPublisher(options ...PublishOption) (*Publisher, error) {
	const errMessage = "failed to create publisher: %w"

	opt := defaultPublishOptions()

	for i := 0; i < len(options); i++ {
		options[i](opt)
	}

	if c.publishConnection == nil {
		c.publishConnection = &connection{
			amqpConnectionMtx:    &sync.Mutex{},
			amqpChannelMtx:       &sync.Mutex{},
			connectionCloseWG:    &sync.WaitGroup{},
			publishersMtx:        &sync.Mutex{},
			publishers:           make(map[string]*Publisher),
			reconnectFailChanMtx: &sync.Mutex{},
			reconnectFailChan:    make(chan error, reconnectFailChanSize),
		}
	}

	publisherName := newRandomString()

	err := connect(c.publishConnection, c.options, c.log, publish)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	publisher := &Publisher{
		channel: c.publishConnection.amqpChannel,
		options: opt,
		encoder: c.options.Codec.Encoder,
		name:    publisherName,
	}

	c.publishConnection.publishersMtx.Lock()
	c.publishConnection.publishers[publisherName] = publisher
	c.publishConnection.publishersMtx.Unlock()

	return publisher, nil
}

// Publish publishes a message with the publish options configured in the Publisher.
//
// target can be a queue name for direct publishing or a routing key.
func (publisher *Publisher) Publish(ctx context.Context, target string, data any) error {
	return publisher.internalPublish(ctx, []string{target}, data, publisher.options)
}

// PublishWithOptions publishes a message to one or multiple targets.
//
// Targets can be a queue names for direct publishing or routing keys.
//
// Options can be passed to override the default options just for this publish.
func (publisher *Publisher) PublishWithOptions(ctx context.Context, targets []string, data any, options ...PublishOption) error {
	const errMessage = "failed to publish message with options: %w"

	// create new options to not override the default options
	opt := *publisher.options

	for i := 0; i < len(options); i++ {
		options[i](&opt)
	}

	if err := publisher.internalPublish(ctx, targets, data, &opt); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
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

		if err := publisher.channel.PublishWithContext(
			ctx,
			options.Exchange,
			key,
			options.Mandatory,
			false, // always set to false since rabbitmq does not support immediate publishing
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

		body, err = publisher.encoder(data)
		if err != nil {
			return nil, fmt.Errorf(errMessage, err)
		}

		if options.ContentType == "" {
			options.ContentType = jsonContentType
		}
	}

	return body, nil
}
