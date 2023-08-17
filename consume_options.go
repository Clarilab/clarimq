package gorabbitmq

import (
	"fmt"
)

const (
	defaultQOSPrefetch int    = 10
	defaultConcurrency int    = 1
	undefinedConsumer  string = "undefined_consumer"
	quorum             string = "quorum"
	maxPriorityKey     string = "x-max-priority"
	queueTypeKey       string = "x-queue-type"
)

type (
	consumeOption func(*ConsumeOptions)

	// ConsumeOptions are used to describe how a new consumer will be configured.
	ConsumeOptions struct {
		ConsumerOptions *ConsumerOptions
		QueueOptions    *QueueOptions
		ExchangeOptions *ExchangeOptions
		Bindings        []Binding
		// The number of message handlers, that will run concurrently.
		HandlerQuantity int
	}

	// ConsumerOptions are used to configure the consumer
	// on the rabbit server.
	ConsumerOptions struct {
		// Application or exchange specific fields,
		// the headers exchange will inspect this field.
		Args Table
		// The name of the consumer / consumer-tag.
		Name string
		// Auto client acknowledgment for each message.
		AutoAck bool
		// Ensures that this is the sole consumer from the queue.
		Exclusive bool
		// If true, the client does not wait for a reply method. If the server could not complete the method it will raise a channel or connection exception.
		NoWait bool
	}
)

// defaultConsumerOptions describes the options that will be used when a value isn't provided.
func defaultConsumerOptions() *ConsumeOptions {
	return &ConsumeOptions{
		ConsumerOptions: &ConsumerOptions{
			Name:      newDefaultConsumerName(),
			AutoAck:   false,
			Exclusive: false,
			NoWait:    false,
			Args:      make(Table),
		},
		QueueOptions:    defaultQueueOptions(),
		ExchangeOptions: defaultExchangeOptions(),
		Bindings:        []Binding{},
		HandlerQuantity: defaultConcurrency,
	}
}

func newDefaultConsumerName() string {
	return fmt.Sprintf("%s_%s", undefinedConsumer, newRandomString())
}

// WithCustomConsumeOptions sets the consumer options.
//
// It can be used to set all consumer options at once.
func WithCustomConsumeOptions(options *ConsumeOptions) consumeOption { //nolint:revive // no need for exported return type
	return func(opt *ConsumeOptions) {
		if options != nil {
			opt.HandlerQuantity = options.HandlerQuantity

			if options.QueueOptions != nil {
				opt.QueueOptions = &QueueOptions{
					Args:       options.QueueOptions.Args,
					Durable:    options.QueueOptions.Durable,
					AutoDelete: options.QueueOptions.AutoDelete,
					Exclusive:  options.QueueOptions.Exclusive,
					NoWait:     options.QueueOptions.NoWait,
					Passive:    options.QueueOptions.Passive,
					Declare:    options.QueueOptions.Declare,
				}
			}

			if options.Bindings != nil {
				opt.Bindings = options.Bindings
			}

			if options.ExchangeOptions != nil {
				opt.ExchangeOptions = options.ExchangeOptions
			}

			if options.ConsumerOptions != nil {
				opt.ConsumerOptions = options.ConsumerOptions
			}
		}
	}
}

// WithQueueOptionDurable sets whether the queue is a durable queue.
//
// Default: false.
func WithQueueOptionDurable(durable bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.QueueOptions.Durable = durable }
}

// WithQueueOptionAutoDelete sets whether the queue is an auto-delete queue.
//
// Default: false.
func WithQueueOptionAutoDelete(autoDelete bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.QueueOptions.AutoDelete = autoDelete }
}

// WithQueueOptionExclusive sets whether the queue is an exclusive queue.
//
// Default: false.
func WithQueueOptionExclusive(exclusive bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.QueueOptions.Exclusive = exclusive }
}

// WithQueueOptionNoWait sets whether the queue is a no-wait queue.
//
// Default: false.
func WithQueueOptionNoWait(noWait bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.QueueOptions.NoWait = noWait }
}

// WithQueueOptionPassive sets whether the queue is a passive queue.
//
// Default: false.
func WithQueueOptionPassive(passive bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.QueueOptions.Passive = passive }
}

// WithQueueOptionDeclare sets whether the queue should be declared upon startup
// if it doesn't already exist.
//
// Default: true.
func WithQueueOptionDeclare(declare bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.QueueOptions.Declare = declare }
}

// WithQueueOptionPriority if set a priority queue will be declared with the
// given maximum priority.
func WithQueueOptionPriority(maxPriority Priority) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) {
		if options.QueueOptions.Args != nil {
			options.QueueOptions.Args[maxPriorityKey] = uint8(maxPriority)
		}
	}
}

// WithQueueOptionArgs adds optional args to the queue.
func WithQueueOptionArgs(args Table) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) {
		if options.QueueOptions.Args != nil {
			options.QueueOptions.Args = args
		}
	}
}

// WithExchangeOptionName sets the exchange name.
func WithExchangeOptionName(name string) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.ExchangeOptions.Name = name }
}

// WithExchangeOptionKind ensures the queue is a durable queue.
func WithExchangeOptionKind(kind string) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.ExchangeOptions.Kind = kind }
}

// WithExchangeOptionDurable sets whether the exchange is a durable exchange.
//
// Default: false.
func WithExchangeOptionDurable(durable bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.ExchangeOptions.Durable = durable }
}

// WithExchangeOptionAutoDelete sets whether the exchange is an auto-delete exchange.
//
// Default: false.
func WithExchangeOptionAutoDelete(autoDelete bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.ExchangeOptions.AutoDelete = autoDelete }
}

// WithExchangeOptionInternal sets whether the exchange is an internal exchange.
//
// Default: false.
func WithExchangeOptionInternal(internal bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.ExchangeOptions.Internal = internal }
}

// WithExchangeOptionNoWait sets whether the exchange is a no-wait exchange.
//
// Default: false.
func WithExchangeOptionNoWait(noWait bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.ExchangeOptions.NoWait = noWait }
}

// WithExchangeOptionDeclare sets whether the exchange should be declared on startup
// if it doesn't already exist.
//
// Default: false.
func WithExchangeOptionDeclare(declare bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.ExchangeOptions.Declare = declare }
}

// WithExchangeOptionPassive sets whether the exchange is a passive exchange.
//
// Default: false.
func WithExchangeOptionPassive(passive bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.ExchangeOptions.Passive = passive }
}

// WithExchangeOptionArgs adds optional args to the exchange.
func WithExchangeOptionArgs(args Table) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) {
		if options.ExchangeOptions.Args != nil {
			options.ExchangeOptions.Args = args
		}
	}
}

// WithConsumerOptionRoutingKey binds the queue to a routing key with the default binding options.
func WithConsumerOptionRoutingKey(routingKey string) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) {
		options.Bindings = append(options.Bindings, Binding{
			RoutingKey:     routingKey,
			BindingOptions: defaultBindingOptions(),
		})
	}
}

// WithConsumerOptionBinding adds a new binding to the queue which allows you to set the binding options
// on a per-binding basis. Keep in mind that everything in the BindingOptions struct will default to
// the zero value. If you want to declare your bindings for example, be sure to set Declare=true.
func WithConsumerOptionBinding(binding Binding) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) {
		options.Bindings = append(options.Bindings, binding)
	}
}

// WithConsumerOptionHandlerQuantity sets the number of message handlers, that will run concurrently.
func WithConsumerOptionHandlerQuantity(concurrency int) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) {
		options.HandlerQuantity = concurrency
	}
}

// WithConsumerOptionConsumerName sets the name on the server of this consumer.
//
// If unset a random name will be given.
func WithConsumerOptionConsumerName(consumerName string) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) {
		options.ConsumerOptions.Name = consumerName
	}
}

// WithConsumerOptionConsumerAutoAck sets the auto acknowledge property on the server of this consumer.
//
// Default: false.
func WithConsumerOptionConsumerAutoAck(autoAck bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.ConsumerOptions.AutoAck = autoAck }
}

// WithConsumerOptionConsumerExclusive sets the exclusive property of this consumer, which means
// the server will ensure that this is the only consumer
// from this queue. When exclusive is false, the server will fairly distribute
// deliveries across multiple consumers.
//
// Default: false.
func WithConsumerOptionConsumerExclusive(exclusive bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.ConsumerOptions.Exclusive = exclusive }
}

// WithConsumerOptionNoWait sets the exclusive nowait property of this consumer, which means
// it does not wait for the server to confirm the request and
// immediately begin deliveries. If it is not possible to consume, a channel
// exception will be raised and the channel will be closed.
//
// Default: false.
func WithConsumerOptionNoWait(noWait bool) consumeOption { //nolint:revive // no need for exported return type
	return func(options *ConsumeOptions) { options.ConsumerOptions.NoWait = noWait }
}

// WithConsumerOptionQueueQuorum sets the queue a quorum type, which means
// multiple nodes in the cluster will have the messages distributed amongst them
// for higher reliability.
func WithConsumerOptionQueueQuorum(options *ConsumeOptions) {
	if options.QueueOptions.Args == nil {
		options.QueueOptions.Args = make(Table)
	}

	options.QueueOptions.Args[queueTypeKey] = quorum
}
