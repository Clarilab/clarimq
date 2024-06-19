package clarimq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type (
	// Consumer is a consumer for AMQP messages.
	Consumer struct {
		conn        *Connection
		channelExec channelExec
		options     *ConsumeOptions
		handler     HandlerFunc
		isConsuming bool
	}

	// Delivery captures the fields for a previously delivered message resident in
	// a queue to be delivered by the broker to a consumer from Channel. Consume or
	// Channel.Get.
	Delivery struct {
		amqp.Delivery
	}

	// HandlerFunc defines the handler of each Delivery and return Action.
	HandlerFunc func(d *Delivery) Action
)

// NewConsumer creates a new Consumer instance. Options can be passed to customize the behavior of the Consumer.
func NewConsumer(conn *Connection, queueName string, handler HandlerFunc, options ...ConsumeOption) (*Consumer, error) {
	const errMessage = "failed to create consumer: %w"

	if conn == nil {
		return nil, fmt.Errorf(errMessage, ErrInvalidConnection)
	}

	opt := defaultConsumerOptions()

	for i := range options {
		options[i](opt)
	}

	opt.QueueOptions.name = queueName

	consumer := &Consumer{
		conn:        conn,
		channelExec: conn.channelExec,
		options:     opt,
		handler:     handler,
	}

	conn.addConsumerRecoveryFN(consumer.options.ConsumerOptions.Name, consumer.recoverConsumer)

	if err := consumer.setupConsumer(); err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	if consumer.options.ConsumeAfterCreation {
		if err := consumer.Start(); err != nil {
			return nil, fmt.Errorf(errMessage, err)
		}
	}

	return consumer, nil
}

// Close stops consuming messages from the subscribed queue.
//
// When using the dead letter retry with enabled cleanup, the consumer must be closed
// to perform the cleanup.
func (c *Consumer) Close() error {
	const errMessage = "failed to unsubscribe consumer: %w"

	if c.options.RetryOptions != nil {
		if err := c.closeDeadLetterRetry(); err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	if err := c.conn.cancelConsumer(c.options.ConsumerOptions.Name); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.isConsuming = false

	return nil
}

func (c *Consumer) setupConsumer() error {
	const errMessage = "failed to setup consumer: %w"

	if err := c.handleDeclarations(
		c.options.ExchangeOptions,
		c.options.QueueOptions,
		c.options.Bindings,
	); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if c.options.RetryOptions != nil {
		if err := c.setupDeadLetterRetry(); err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	return nil
}

// Start starts consuming messages from the subscribed queue.
func (c *Consumer) Start() error {
	const errMessage = "failed to start: %w"

	if c.isConsuming {
		return fmt.Errorf(errMessage, ErrConsumerAlreadyRunning)
	}

	return c.startConsuming()
}

func (c *Consumer) startConsuming() error {
	const errMessage = "failed to start consuming: %w"

	var deliveries <-chan amqp.Delivery

	if err := c.channelExec(func(channel *amqp.Channel) error {
		var err error

		deliveries, err = channel.Consume(
			c.options.QueueOptions.name,
			c.options.ConsumerOptions.Name,
			c.options.ConsumerOptions.AutoAck,
			c.options.ConsumerOptions.Exclusive,
			false, // always set to false since RabbitMQ does not support immediate publishing
			c.options.ConsumerOptions.NoWait,
			amqp.Table(c.options.ConsumerOptions.Args),
		)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	for range c.options.HandlerQuantity {
		go c.handlerRoutine(deliveries)
	}

	c.isConsuming = true

	c.conn.logger.logDebug(context.Background(), fmt.Sprintf("Processing messages on %d message handlers", c.options.HandlerQuantity))

	return nil
}

func (c *Consumer) handleDeclarations(exchangeOptions *ExchangeOptions, queueOptions *QueueOptions, bindings []Binding) error {
	const errMessage = "failed to handle declarations: %w"

	if err := declareExchange(c.channelExec, exchangeOptions); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if err := declareQueue(c.channelExec, queueOptions); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if err := declareBindings(c.channelExec, queueOptions.name, exchangeOptions.Name, bindings); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (c *Consumer) handlerRoutine(deliveries <-chan amqp.Delivery) {
	for delivery := range deliveries {
		delivery := &Delivery{delivery}

		if c.conn.isClosed() {
			c.conn.logger.logDebug(context.Background(), "message handler stopped: channel is closed")

			break
		}

		if c.options.ConsumerOptions.AutoAck {
			c.handler(delivery)

			continue
		}

		switch c.handleMessage(delivery) {
		case Ack:
			if err := delivery.Ack(false); err != nil {
				c.conn.logger.logError(context.Background(), "could not ack message: %v", err)
			}

		case NackDiscard:
			if err := delivery.Nack(false, false); err != nil {
				c.conn.logger.logError(context.Background(), "could not nack message: %v", err)
			}

		case NackRequeue:
			if err := delivery.Nack(false, true); err != nil {
				c.conn.logger.logError(context.Background(), "could not nack message: %v", err)
			}

		case Manual:
			continue
		}
	}
}

func (c *Consumer) handleMessage(delivery *Delivery) Action {
	var err error

	action := c.handler(delivery)

	if action == NackDiscard && c.options.RetryOptions != nil {
		if action, err = c.handleDeadLetterMessage(delivery); err != nil {
			c.conn.logger.logError(context.Background(), "could not handle dead letter message: %v", err)
		}
	}

	return action
}

func (c *Consumer) recoverConsumer() error {
	const errMessage = "failed to recover consumer: %w"

	if err := c.setupConsumer(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if err := c.startConsuming(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}
