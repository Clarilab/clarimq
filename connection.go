package gorabbitmq

import (
	"fmt"
	"maps"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type connection struct {
	amqpConnectionMtx *sync.Mutex
	amqpConnection    *amqp.Connection

	amqpChannelMtx *sync.Mutex
	amqpChannel    *amqp.Channel

	connectionCloseWG *sync.WaitGroup

	reconnectChan        chan struct{}
	reconnectFailChanMtx *sync.Mutex
	reconnectFailChan    chan error

	consumerCloseChan chan string
	consumersMtx      *sync.Mutex
	consumers         map[string]*Consumer

	publishersMtx *sync.Mutex
	publishers    map[string]*Publisher
}

func (c *connection) watchConsumerClose() {
	go func() {
		for consumer := range c.consumerCloseChan {
			c.consumersMtx.Lock()

			maps.DeleteFunc(c.consumers, func(k string, v *Consumer) bool {
				return consumer == k
			})

			c.consumersMtx.Unlock()
		}
	}()
}

func (c *connection) watchReconnects(instanceType string, opt *ConnectorOptions, logger *log) {
	go func() {
		for range c.reconnectChan {
			err := c.reconnect(instanceType, opt, logger)
			if err != nil {
				c.reconnectFailChanMtx.Lock()
				c.reconnectFailChan <- err
				c.reconnectFailChanMtx.Unlock()

				return
			}
		}
	}()
}

func (c *connection) recoverPublishers(logger *log) {
	if c.publishers != nil {
		c.publishersMtx.Lock()

		for i := range c.publishers {
			publisher := c.publishers[i]

			publisher.channel = c.amqpChannel
		}

		logger.logDebug("successfully recovered publisher(s)", "publisherCount", len(c.publishers))

		c.publishersMtx.Unlock()
	}
}

func (c *connection) reconnect(instanceType string, opt *ConnectorOptions, logger *log) error {
	err := backoff(
		func() error {
			err := createConnection(c, opt, instanceType, logger)
			if err != nil {
				return err
			}

			err = createChannel(c, opt, instanceType, logger)
			if err != nil {
				return err
			}

			return nil
		},
		&backoffParams{
			instanceType: instanceType,
			initDelay:    opt.ReconnectInterval,
			maxRetries:   opt.MaxReconnectRetries,
			factor:       opt.BackoffFactor,
		},
		logger,
	)
	if err != nil {
		c.amqpConnectionMtx.Lock()
		c.amqpConnection = nil
		c.amqpConnectionMtx.Unlock()

		c.amqpChannelMtx.Lock()
		c.amqpChannel = nil
		c.amqpChannelMtx.Unlock()

		return err
	}

	c.recoverPublishers(logger)

	err = c.recoverConsumers(logger)
	if err != nil {
		return err
	}

	logger.logInfo(fmt.Sprintf("successfully reconnected %s connection", instanceType))

	return nil
}

func (c *connection) recoverConsumers(logger *log) error {
	const errMessage = "failed to recover consumers %w"

	if c.consumers != nil {
		c.consumersMtx.Lock()

		for i := range c.consumers {
			consumer := c.consumers[i]

			consumer.channel = c.amqpChannel

			err := consumer.startConsuming()
			if err != nil {
				return fmt.Errorf(errMessage, err)
			}
		}

		logger.logDebug("successfully recovered consumer(s)", "consumerCount", len(c.consumers))

		c.consumersMtx.Unlock()
	}

	return nil
}

type backoffParams struct {
	instanceType string
	initDelay    time.Duration
	factor       int
	maxRetries   int
}

func backoff(action func() error, params *backoffParams, logger *log) error {
	const errMessage = "backoff failed %w"

	retry := 0

	for retry <= params.maxRetries {
		if action() == nil {
			logger.logDebug(fmt.Sprintf("successfully reestablished %s connection", params.instanceType), "retries", retry)

			break
		}

		if retry == params.maxRetries {
			logger.logDebug(fmt.Sprintf("%s reconnection failed: maximum retries exceeded", params.instanceType), "retries", retry)

			return fmt.Errorf(errMessage, ErrMaxRetriesExceeded)
		}

		delay := time.Duration(params.factor*retry) * params.initDelay

		logger.logDebug("failed to reconnect: backing off...", "backoff-time", delay.String())

		time.Sleep(delay)

		retry++
	}

	return nil
}
