package clarimq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// QueueInfo represents the current server state of a queue on the server.
type QueueInfo amqp.Queue

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
	// If true, the client does not wait for a reply method. If the broker could not complete the method it will raise a channel or connection exception.
	NoWait bool
	// If false, a missing queue will be created on the broker.
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

func declareQueue(channelExec channelExec, options *QueueOptions) error {
	const errMessage = "failed to declare queue: %w"

	if !options.Declare {
		return nil
	}

	if err := channelExec(func(channel *amqp.Channel) error {
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

			return err //nolint:wrapcheck // intended
		}

		_, err = channel.QueueDeclare(
			options.name,
			options.Durable,
			options.AutoDelete,
			options.Exclusive,
			options.NoWait,
			amqp.Table(options.Args),
		)

		return err //nolint:wrapcheck // intended
	}); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func getQueueInfo(channelExec channelExec, name string) (*QueueInfo, error) {
	const errMessage = "failed to get queue info: %w"

	result := new(QueueInfo)

	exec := func(channel *amqp.Channel) error {
		q, err := channel.QueueDeclarePassive(name, false, false, false, false, nil)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		*result = QueueInfo(q)

		return nil
	}

	if err := channelExec(exec); err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	return result, nil
}
