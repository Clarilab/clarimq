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
	HandlerFunc func(d Delivery) Action
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
func (c *Consumer) Close() error {
	const errMessage = "failed to unsubscribe consumer: %w"

	if err := c.conn.amqpChannel.Cancel(c.options.ConsumerOptions.Name, false); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.conn.runningConsumers--

	return nil
}

// DecodeDeliveryBody can be used to decode the body of a delivery into v.
func (c *Consumer) DecodeDeliveryBody(delivery Delivery, v any) error {
	const errMessage = "failed to decode delivery body: %w"

	if err := c.conn.options.Codec.Decoder(delivery.Body, v); err != nil {
		return fmt.Errorf(errMessage, err)
	}

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

	err = declareBindings(c.conn.amqpChannel, c.options)
	if err != nil {
		return fmt.Errorf(errMessage, err)
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
		go c.handlerRoutine(deliveries, c.options, c.handler)
	}

	c.conn.logger.logDebug(fmt.Sprintf("Processing messages on %d message handlers", c.options.HandlerQuantity))

	return nil
}

func (c *Consumer) handlerRoutine(deliveries <-chan amqp.Delivery, consumeOptions *ConsumeOptions, handler HandlerFunc) {
	for msg := range deliveries {
		if c.conn.amqpChannel.IsClosed() {
			c.conn.logger.logDebug("message handler stopped: channel is closed")

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
				c.conn.logger.logError("could not ack message: %v", err)
			}

		case NackDiscard:
			err := msg.Nack(false, false)
			if err != nil {
				c.conn.logger.logError("could not nack message: %v", err)
			}

		case NackRequeue:
			err := msg.Nack(false, true)
			if err != nil {
				c.conn.logger.logError("could not nack message: %v", err)
			}

		case Manual:
			continue
		}
	}
}

func (c *Consumer) watchRecoverConsumerChan() {
	go func() {
		for range c.conn.consumerRecoveryChan {
			c.conn.consumerRecoveryChan <- c.startConsuming()
		}
	}()
}
