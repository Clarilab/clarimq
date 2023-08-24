package gorabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// QueueOptions are used to configure a queue.
// A passive queue is assumed by RabbitMQ to already exist, and attempting to connect
// to a non-existent queue will cause RabbitMQ to throw an exception.
type QueueOptions struct {
	// Are used by plugins and broker-specific features such as message TTL, queue length limit, etc.
	Args Table
	// Queue name.
	name string
	// If true, the queue will survive a broker restart.
	Durable bool
	// If true, the queue is deleted when last consumer unsubscribes.
	AutoDelete bool
	// If true, the queue is used by only one connection and will be deleted when that connection closes.
	Exclusive bool
	// If true, the client does not wait for a reply method. If the server could not complete the method it will raise a channel or connection exception.
	NoWait bool
	// If false, a missing queue will be created on the server.
	Passive bool
	// If true, the queue will be declared if it does not already exist.
	Declare bool
}

func defaultQueueOptions() *QueueOptions {
	return &QueueOptions{
		Args:       make(Table),
		name:       "",
		Durable:    false,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Passive:    false,
		Declare:    true,
	}
}

func declareQueue(channel *amqp.Channel, options *QueueOptions) error {
	const errMessage = "failed to declare queue: %w"

	if !options.Declare {
		return nil
	}

	var err error

	if options.Passive {
		_, err = channel.QueueDeclarePassive(
			options.name,
			options.Durable,
			options.AutoDelete,
			options.Exclusive,
			options.NoWait,
			amqp.Table(options.Args),
		)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		return nil
	}

	_, err = channel.QueueDeclare(
		options.name,
		options.Durable,
		options.AutoDelete,
		options.Exclusive,
		options.NoWait,
		amqp.Table(options.Args),
	)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}
