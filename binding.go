package clarimq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type (
	// Binding describes the binding of a queue to a routing key to an exchange.
	Binding struct {
		*BindingOptions
		RoutingKey   string
		QueueName    string
		ExchangeName string
	}

	// BindingOptions describes the options a binding can have.
	BindingOptions struct {
		// Are used by plugins and broker-specific features such as message TTL, queue length limit, etc.
		Args Table
		// If true, the client does not wait for a reply method. If the server could not complete the method it will raise a channel or connection exception.
		NoWait bool
		// If true, the binding will be declared if it does not already exist.
		Declare bool
	}
)

func (b *Binding) defaultQueueNameOr(name string) string {
	if b.QueueName != "" {
		return b.QueueName
	}

	return name
}

func (b *Binding) defaultExchangeNameOr(name string) string {
	if b.ExchangeName != "" {
		return b.ExchangeName
	}

	return name
}

func defaultBindingOptions() *BindingOptions {
	return &BindingOptions{
		Args:    make(Table),
		NoWait:  false,
		Declare: true,
	}
}

func declareBindings(channel *amqp.Channel, queueName, exchangeName string, bindings []Binding) error {
	const errMessage = "failed to declare binding: %w"

	for _, binding := range bindings {
		if !binding.Declare {
			continue
		}

		err := channel.QueueBind(
			binding.defaultQueueNameOr(queueName),
			binding.RoutingKey,
			binding.defaultExchangeNameOr(exchangeName),
			binding.NoWait,
			amqp.Table(binding.Args),
		)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	return nil
}
