package gorabbitmq

import (
	"errors"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	consumeType string = "consume"
)

type (

	// Consumer is a consumer for AMQP messages.
	Consumer struct {
		connector  *Connector
		options    *ConsumeOptions
		subscribed bool
	}

	// Delivery captures the fields for a previously delivered message resident in
	// a queue to be delivered by the server to a consumer from Channel. Consume or
	// Channel.Get.
	Delivery struct {
		amqp.Delivery
	}

	// HandlerFunc defines the handler of each Delivery and return Action.
	HandlerFunc func(d Delivery) Action
)

// NewConsumer creates a new Consumer instance. Options can be passed to customize the behavior of the Consumer.
func (c *Connector) NewConsumer(queueName string, options ...consumeOption) (*Consumer, error) {
	const errMessage = "failed to create consumer: %w"

	opt := defaultConsumerOptions()

	for i := 0; i < len(options); i++ {
		options[i](opt)
	}

	opt.QueueOptions.name = queueName

	var err error

	c.consumeConn, c.consumeChannel, err = connect(&connectParams{
		instanceType: consumeType,
		conn:         c.consumeConn,
		channel:      c.consumeChannel,
		opt:          c.options,
		closeWG:      c.consumeCloseWG,
	})
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	return &Consumer{
		connector:  c,
		options:    opt,
		subscribed: false,
	}, nil
}

// NewConsumer creates a new Consumer instance which immediately starts subscribing to the given queue.
//
// Options can be passed to customize the behavior of the Consumer.
func (c *Connector) NewConsumerAndSubscribe(queueName string, handler HandlerFunc, options ...consumeOption) (*Consumer, error) {
	const errMessage = "failed to create consumer and subscribe: %w"

	consumer, err := c.NewConsumer(queueName, options...)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	if err = consumer.Subscribe(handler); err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	return consumer, nil
}

// Subscribe starts consuming messages from the queue with the specified consumer options.
//
// The handler function is called for each message to handle the delivery received from the queue.
func (c *Consumer) Subscribe(handler HandlerFunc) error {
	const errMessage = "failed to subscribe consumer: %w"

	if err := c.startConsuming(handler); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

// Subscribe stops consuming messages from the subscribed queue.
func (c *Consumer) Unsubscribe() error {
	const errMessage = "failed to unsubscribe consumer: %w"

	if err := c.connector.consumeChannel.Cancel(c.options.ConsumerOptions.Name, false); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.subscribed = false

	return nil
}

// RemoveQueue removes the queue from the server including all bindings then purges the messages based on
// server configuration, returning the number of messages purged.
//
// When ifUnused is true, the queue will not be deleted if there are any consumers on the queue.
// If there are consumers, an error will be returned and the channel will be closed.
//
// When ifEmpty is true, the queue will not be deleted if there are any messages remaining on the queue.
// If there are messages, an error will be returned and the channel will be closed.
func (c *Consumer) RemoveQueue(name string, ifUnused bool, ifEmpty bool, noWait bool) (int, error) {
	const errMessage = "failed to remove queue: %w"

	removedMessages, err := c.connector.consumeChannel.QueueDelete(name, ifUnused, ifEmpty, noWait)
	if err != nil {
		return 0, fmt.Errorf(errMessage, err)
	}

	return removedMessages, nil
}

// RemoveBinding removes a binding between an exchange and queue matching the key and arguments.
//
// It is possible to send and empty string for the exchange name which means to unbind the queue from the default exchange.
func (c *Consumer) RemoveBinding(queueName string, routingKey string, exchangeName string, args Table) error {
	const errMessage = "failed to remove binding: %w"

	err := c.connector.consumeChannel.QueueUnbind(queueName, routingKey, exchangeName, amqp.Table(args))
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

// ExchangeDelete removes the named exchange from the server. When an exchange is deleted all queue bindings
// on the exchange are also deleted. If this exchange does not exist, the channel will be closed with an error.
//
// When ifUnused is true, the server will only delete the exchange if it has no queue bindings.
// If the exchange has queue bindings the server does not delete it but close the channel with an exception instead.
// Set this to true if you are not the sole owner of the exchange.
//
// When noWait is true, do not wait for a server confirmation that the exchange has been deleted.
// Failing to delete the channel could close the channel. Add a NotifyClose listener to respond to these channel exceptions.
func (c *Consumer) RemoveExchange(name string, ifUnused bool, noWait bool) error {
	const errMessage = "failed to remove exchange: %w"

	err := c.connector.consumeChannel.ExchangeDelete(name, ifUnused, noWait)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

// ErrAlreadySubscribed occours when the consumer is already subscribed to a queue.
var ErrAlreadySubscribed = errors.New("consumer already has an active subscription")

func (c *Consumer) startConsuming(handler HandlerFunc) error {
	const errMessage = "failed to start consuming: %w"

	if c.subscribed {
		return fmt.Errorf(errMessage, ErrAlreadySubscribed)
	}

	err := declareExchange(c.connector.consumeChannel, c.options.ExchangeOptions)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	err = c.connector.declareQueue(c.options.QueueOptions)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	err = declareBindings(c.connector.consumeChannel, c.options)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	deliveries, err := c.connector.consumeChannel.Consume(
		c.options.QueueOptions.name,
		c.options.ConsumerOptions.Name,
		c.options.ConsumerOptions.AutoAck,
		c.options.ConsumerOptions.Exclusive,
		false, // not supported by RabbitMQ
		c.options.ConsumerOptions.NoWait,
		amqp.Table(c.options.ConsumerOptions.Args),
	)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	for i := 0; i < c.options.HandlerQuantity; i++ {
		go c.handlerRoutine(deliveries, c.options, handler)
	}

	c.subscribed = true

	slog.Debug(fmt.Sprintf("Processing messages on %d message handlers", c.options.HandlerQuantity))

	return nil
}

func (c *Consumer) handlerRoutine(deliveries <-chan amqp.Delivery, consumeOptions *ConsumeOptions, handler HandlerFunc) {
	for msg := range deliveries {
		if c.connector.consumeChannel.IsClosed() {
			break
		}

		if consumeOptions.ConsumerOptions.AutoAck {
			handler(Delivery{msg})

			continue
		}

		switch handler(Delivery{msg}) {
		case Ack:
			err := msg.Ack(false)
			if err != nil {
				slog.Error("could not ack message: %v", err)
			}

		case NackDiscard:
			err := msg.Nack(false, false)
			if err != nil {
				slog.Error("could not nack message: %v", err)
			}

		case NackRequeue:
			err := msg.Nack(false, true)
			if err != nil {
				slog.Error("could not nack message: %v", err)
			}

		case Manual:
			continue
		}
	}
}
