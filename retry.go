package clarimq

import (
	"context"
	"fmt"
	"time"
)

const (
	ArgDLX string = "x-dead-letter-exchange"
	ArgDLK string = "x-dead-letter-routing-key"
	ArgTTL string = "x-message-ttl"

	keyRetryCount string = "x-retry-count"
	keyRetry      string = "retry"
	requeueSuffix string = "_requeue"
	dlxPrefix     string = "dlx_"
)

func defaultDLXOptions(dlxName string) *ExchangeOptions {
	return &ExchangeOptions{
		Declare:    true,
		Name:       dlxName,
		Kind:       ExchangeDirect,
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Passive:    false,
		Args:       nil,
	}
}

func defaultDLQOptions(name, dlxName, routingKey string, ttl *time.Duration) *QueueOptions {
	return &QueueOptions{
		Declare:    true,
		name:       name,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Passive:    false,
		Args: map[string]any{
			ArgDLX: dlxName, // original exchange, in which the event gets retried
			ArgDLK: routingKey,
			ArgTTL: ttl.Milliseconds(),
		},
	}
}

func (c *Consumer) setupDeadLetterRetry() error {
	const errMessage = "failed to setup dead letter exchange retry %w"

	if err := c.setupRetryPublisher(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.options.RetryOptions.dlxName = dlxPrefix + c.options.ExchangeOptions.Name
	c.options.RetryOptions.dlqNameBase = dlxPrefix + c.options.QueueOptions.name

	if err := declareExchange(c.options.RetryOptions.RetryConn.channelExec,
		defaultDLXOptions(c.options.RetryOptions.dlxName),
	); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if err := c.setupDeadLetterQueues(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (c *Consumer) setupRetryPublisher() error {
	const errMessage = "failed to setup retry publisher %w"

	var err error

	if c.options.RetryOptions.RetryConn == nil {
		c.conn.options.uriMU.RLock()
		uri := c.conn.options.uri
		c.conn.options.uriMU.RUnlock()

		if c.options.RetryOptions.RetryConn, err = NewConnection(
			uri,
			WithConnectionOptionConnectionName(fmt.Sprintf("%s_%s", c.options.ConsumerOptions.Name, keyRetry)),
		); err != nil {
			return fmt.Errorf(errMessage, err)
		}

		c.options.RetryOptions.isInternalConn = true
	}

	if c.options.RetryOptions.publisher, err = NewPublisher(c.options.RetryOptions.RetryConn); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (c *Consumer) setupDeadLetterQueues() error {
	const errMessage = "failed to setup dead letter queues %w"

	routingKey := c.options.QueueOptions.name + requeueSuffix

	// allocating for each retry delay + the binding to the original queue
	bindings := make([]Binding, 0, len(c.options.RetryOptions.Delays)+1)

	// declare and bind queues with ttl values
	for i := range c.options.RetryOptions.Delays {
		ttl := &c.options.RetryOptions.Delays[i]

		queueName := fmt.Sprintf("%s_%s", c.options.RetryOptions.dlqNameBase, ttl.String())

		if err := declareQueue(c.options.RetryOptions.RetryConn.channelExec,
			defaultDLQOptions(
				queueName,
				c.options.ExchangeOptions.Name,
				routingKey,
				ttl,
			),
		); err != nil {
			return fmt.Errorf(errMessage, err)
		}

		bindings = append(bindings, Binding{
			BindingOptions: defaultBindingOptions(),
			RoutingKey:     queueName,
			QueueName:      queueName,
			ExchangeName:   c.options.RetryOptions.dlxName,
		})
	}

	// append binding for the original queue
	bindings = append(bindings,
		Binding{
			BindingOptions: defaultBindingOptions(),
			RoutingKey:     routingKey,
			ExchangeName:   c.options.ExchangeOptions.Name,
			QueueName:      c.options.QueueOptions.name,
		},
	)

	if err := declareBindings(c.options.RetryOptions.RetryConn.channelExec, "", "", bindings); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (c *Consumer) handleDeadLetterMessage(
	delivery *Delivery,
) (Action, error) {
	const errMessage = "failed to handle dead letter exchange message %w"

	retryCount, ok := delivery.Headers[keyRetryCount].(int64)
	if !ok {
		retryCount = 0
	}

	maxRetriesExceeded := retryCount >= c.options.RetryOptions.MaxRetries

	switch {
	case maxRetriesExceeded && c.options.RetryOptions.MaxRetriesExceededHandler != nil:
		if err := c.options.RetryOptions.MaxRetriesExceededHandler(delivery); err != nil {
			return NackDiscard, fmt.Errorf(errMessage, err)
		}

		fallthrough

	case maxRetriesExceeded:
		return NackDiscard, nil
	}

	// if retryCount exceeds number of delays, use the last defined delay value
	ttl := c.options.RetryOptions.Delays[min(int(retryCount), len(c.options.RetryOptions.Delays)-1)]

	if delivery.Headers == nil {
		delivery.Headers = make(map[string]any)
	}

	delivery.Headers[keyRetryCount] = retryCount + 1

	// publish delivery to the next retry queue
	if err := c.options.RetryOptions.publisher.PublishWithOptions(
		context.Background(),
		[]string{fmt.Sprintf("%s_%s", c.options.RetryOptions.dlqNameBase, ttl.String())},
		delivery.Body,
		WithPublishOptionMandatory(true),
		WithPublishOptionDeliveryMode(PersistentDelivery),
		WithPublishOptionExchange(c.options.RetryOptions.dlxName),
		WithPublishOptionHeaders(Table(delivery.Headers)),
	); err != nil {
		return NackRequeue, fmt.Errorf(errMessage, err)
	}

	// acknowledge delivery to the original queue
	return Ack, nil
}

func (c *Consumer) closeDeadLetterRetry() error {
	const errMessage = "failed to close dead letter exchange retry %w"

	if c.options.RetryOptions.Cleanup {
		if err := c.cleanupDeadLetterRetry(); err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	if c.options.RetryOptions.isInternalConn {
		if err := c.options.RetryOptions.RetryConn.Close(); err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	return nil
}

func (c *Consumer) cleanupDeadLetterRetry() error {
	const errMessage = "failed to cleanup dead letter exchange retry %w"

	// remove the dead letter exchange
	if err := c.options.RetryOptions.RetryConn.amqpChannel.ExchangeDelete(c.options.RetryOptions.dlxName, false, false); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	// remove all dead letter queues
	for i := range c.options.RetryOptions.Delays {
		ttl := &c.options.RetryOptions.Delays[i]

		queueName := fmt.Sprintf("%s_%s", c.options.RetryOptions.dlqNameBase, ttl.String())

		removed, err := c.options.RetryOptions.RetryConn.amqpChannel.QueueDelete(queueName, false, false, false)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		c.conn.logger.logDebug(context.Background(), "dead letter queue removed due to cleanup", "purgedMessages", removed)
	}

	return nil
}
