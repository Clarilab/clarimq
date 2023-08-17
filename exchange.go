package gorabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ExchangeOptions are used to configure an exchange.
// If the Passive flag is set the client will only check if the exchange exists on the server
// and that the settings match, no creation attempt will be made.
type ExchangeOptions struct {
	// Are used by plugins and broker-specific features such as message TTL, queue length limit, etc.
	Args Table
	// Exchange name.
	Name string
	// Exchange type. Possible values: empty string for default exchange or direct, topic, fanout
	Kind string
	// If true, the exchange survives broker restart.
	Durable bool
	// If true, the exchange is deleted when last queue is unbound from it.
	AutoDelete bool
	// If yes, clients cannot publish to this exchange directly. It can only be used with exchange to exchange bindings.
	Internal bool
	// If true, the client does not wait for a reply method. If the server could not complete the method it will raise a channel or connection exception.
	NoWait bool
	// If false, a missing exchange will be created on the server
	Passive bool
	// If true, the exchange will be created only if it does not already exist.
	Declare bool
}

func defaultExchangeOptions() *ExchangeOptions {
	return &ExchangeOptions{
		Args:       make(Table),
		Name:       ExchangeDefault,
		Kind:       amqp.ExchangeDirect,
		Durable:    false,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Passive:    false,
		Declare:    false,
	}
}

func declareExchange(channel *amqp.Channel, options *ExchangeOptions) error {
	const errMessage = "failed to declare exchange: %w"

	if !options.Declare {
		return nil
	}

	if options.Passive {
		err := channel.ExchangeDeclarePassive(
			options.Name,
			options.Kind,
			options.Durable,
			options.AutoDelete,
			options.Internal,
			options.NoWait,
			amqp.Table(options.Args),
		)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		return nil
	}

	err := channel.ExchangeDeclare(
		options.Name,
		options.Kind,
		options.Durable,
		options.AutoDelete,
		options.Internal,
		options.NoWait,
		amqp.Table(options.Args),
	)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}
