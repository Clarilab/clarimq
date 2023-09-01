package gorabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type (
	// Consumer is a consumer for AMQP messages.
	Consumer struct {
		conn    *Connection
		options *ConsumeOptions
		handler HandlerFunc
	}

	// Delivery captures the fields for a previously delivered message resident in
	// a queue to be delivered by the server to a consumer from Channel. Consume or
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

	for i := 0; i < len(options); i++ {
		options[i](opt)
	}

	opt.QueueOptions.name = queueName

	consumer := &Consumer{
		conn:    conn,
		options: opt,
		handler: handler,
	}

	if err := consumer.startConsuming(); err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	consumer.watchRecoverConsumerChan()

	consumer.conn.runningConsumers++

	return consumer, nil
}

// Close stops consuming messages from the subscribed queue.
//
// When using the dead letter retry with enabled cleanup, the consumer must be closed
// to perform the cleanup.
func (c *Consumer) Close() error {
	const errMessage = "failed to unsubscribe consumer: %w"

	var err error

	if c.options.RetryOptions != nil {
		err = c.closeDeadLetterRetry()
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	if err = c.conn.amqpChannel.Cancel(c.options.ConsumerOptions.Name, false); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.conn.runningConsumers--

	return nil
}

func (c *Consumer) startConsuming() error {
	const errMessage = "failed to start consuming: %w"

	err := declareExchange(c.conn.amqpChannel, c.options.ExchangeOptions)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	err = declareQueue(c.conn.amqpChannel, c.options.QueueOptions)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	err = declareBindings(c.conn.amqpChannel, c.options.QueueOptions.name, c.options.ExchangeOptions.Name, c.options.Bindings)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if c.options.RetryOptions != nil {
		err = c.setupDeadLetterRetry()
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	deliveries, err := c.conn.amqpChannel.Consume(
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
		go c.handlerRoutine(deliveries)
	}

	c.conn.logger.logDebug(fmt.Sprintf("Processing messages on %d message handlers", c.options.HandlerQuantity))

	return nil
}

func (c *Consumer) handlerRoutine(deliveries <-chan amqp.Delivery) {
	for delivery := range deliveries {
		delivery := &Delivery{delivery}

		if c.conn.amqpChannel.IsClosed() {
			c.conn.logger.logDebug("message handler stopped: channel is closed")

			break
		}

		if c.options.ConsumerOptions.AutoAck {
			c.handler(delivery)

			continue
		}

		switch c.handleMessage(delivery) {
		case Ack:
			err := delivery.Ack(false)
			if err != nil {
				c.conn.logger.logError("could not ack message: %v", err)
			}

		case NackDiscard:
			err := delivery.Nack(false, false)
			if err != nil {
				c.conn.logger.logError("could not nack message: %v", err)
			}

		case NackRequeue:
			err := delivery.Nack(false, true)
			if err != nil {
				c.conn.logger.logError("could not nack message: %v", err)
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
		action, err = c.handleDeadLetterMessage(delivery)
		if err != nil {
			c.conn.logger.logError("could not handle dead letter message: %v", err)
		}
	}

	return action
}

func (c *Consumer) watchRecoverConsumerChan() {
	go func() {
		for range c.conn.consumerRecoveryChan {
			c.conn.consumerRecoveryChan <- c.startConsuming()
		}
	}()
}
