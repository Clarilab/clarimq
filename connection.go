package clarimq

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	closeWGDelta int    = 2
	errChanSize  int    = 32
	endOfFile    string = "EOF"
)

type Connection struct {
	options *ConnectionOptions

	amqpConnMU     sync.Mutex
	amqpConnection *amqp.Connection

	amqpChanMU  sync.Mutex
	amqpChannel *amqp.Channel

	connectionCloseWG *sync.WaitGroup

	errChanMU sync.Mutex
	errChan   chan error

	consumerRecoverFNsMtx sync.RWMutex
	consumerRecoverFNs    map[string]func() error

	publisherCheckCacheFNsMtx sync.RWMutex
	publisherCheckCacheFNs    map[string]func()

	logger *logger

	returnHandler ReturnHandler
}

// NewConnection creates a new connection.
//
// Must be closed with the Close() method to conserve resources!
func NewConnection(uri string, options ...ConnectionOption) (*Connection, error) {
	const errMessage = "failed to create connection %w"

	opt := defaultConnectionOptions(uri)

	for i := range options {
		options[i](opt)
	}

	conn := &Connection{
		connectionCloseWG:      &sync.WaitGroup{},
		errChan:                make(chan error, errChanSize),
		consumerRecoverFNs:     make(map[string]func() error),
		publisherCheckCacheFNs: make(map[string]func()),
		logger:                 newLogger(opt.loggers),
		returnHandler:          opt.ReturnHandler,
		options:                opt,
	}

	if err := conn.connect(); err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	return conn, nil
}

// SettingsToURI can be used to convert a ConnectionSettings struct to a valid AMQP URI to ensure correct escaping.
func SettingsToURI(settings *ConnectionSettings) string {
	return fmt.Sprintf("amqp://%s:%s@%s/",
		url.QueryEscape(settings.UserName),
		url.QueryEscape(settings.Password),
		net.JoinHostPort(
			url.QueryEscape(settings.Host),
			strconv.Itoa(settings.Port),
		),
	)
}

// Close gracefully closes the connection to the broker.
func (c *Connection) Close() error {
	const errMessage = "failed to close the connection to the broker gracefully: %w"

	if c.amqpConnection != nil {
		logCtx := context.Background()

		c.logger.logDebug(logCtx, "closing connection")

		c.connectionCloseWG.Add(closeWGDelta)

		if err := c.amqpConnection.Close(); err != nil {
			return fmt.Errorf(errMessage, err)
		}

		c.connectionCloseWG.Wait()

		close(c.errChan)

		c.logger.logInfo(logCtx, "gracefully closed connection to the broker")
	}

	return nil
}

// Name returns the name of the connection if specified, otherwise returns an empty string.
func (c *Connection) Name() string {
	if name, ok := c.options.Config.Properties[amqpConnectionNameKey].(string); ok {
		return name
	}

	return ""
}

// NotifyErrors returns a channel that will return an errors that happen concurrently.
func (c *Connection) NotifyErrors() <-chan error {
	return c.errChan
}

// Recover can be used to manually start the recovery.
func (c *Connection) Recover() error {
	const errMessage = "failed to recover: %w"

	if err := c.recoverConnection(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if err := c.recoverChannel(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

// Renew can be used to establish a new connection.
// If new URI is provided, it will be used to renew the connection instead of the current URI.
func (c *Connection) Renew(uri ...string) error {
	const errMessage = "failed to renew: %w"

	if len(uri) == 1 {
		c.options.uriMU.Lock()
		c.options.uri = uri[0]
		c.options.uriMU.Unlock()
	}

	if err := c.closeForRenewal(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if err := c.recoverConnection(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if err := c.recoverChannel(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (c *Connection) closeForRenewal() error {
	const errMessage = "failed to close the connection to the broker gracefully on renewal: %w"

	if c.amqpConnection != nil {
		logCtx := context.Background()

		c.logger.logDebug(logCtx, "closing connection")

		c.connectionCloseWG.Add(closeWGDelta)

		if err := c.amqpConnection.Close(); err != nil {
			return fmt.Errorf(errMessage, err)
		}

		c.connectionCloseWG.Wait()

		c.logger.logDebug(logCtx, "gracefully closed connection to the broker")
	}

	return nil
}

// RemoveQueue removes the queue from the broker including all bindings then purges the messages based on
// broker configuration, returning the number of messages purged.
//
// When ifUnused is true, the queue will not be deleted if there are any consumers on the queue.
// If there are consumers, an error will be returned and the channel will be closed.
//
// When ifEmpty is true, the queue will not be deleted if there are any messages remaining on the queue.
// If there are messages, an error will be returned and the channel will be closed.
func (c *Connection) RemoveQueue(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	const errMessage = "failed to remove queue: %w"

	purgedMessages, err := c.amqpChannel.QueueDelete(name, ifUnused, ifEmpty, noWait)
	if err != nil {
		return 0, fmt.Errorf(errMessage, err)
	}

	return purgedMessages, nil
}

// RemoveBinding removes a binding between an exchange and queue matching the key and arguments.
//
// It is possible to send and empty string for the exchange name which means to unbind the queue from the default exchange.
func (c *Connection) RemoveBinding(queueName, routingKey, exchangeName string, args Table) error {
	const errMessage = "failed to remove binding: %w"

	if err := c.amqpChannel.QueueUnbind(queueName, routingKey, exchangeName, amqp.Table(args)); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

// RemoveExchange removes the named exchange from the broker. When an exchange is deleted all queue bindings
// on the exchange are also deleted. If this exchange does not exist, the channel will be closed with an error.
//
// When ifUnused is true, the broker will only delete the exchange if it has no queue bindings.
// If the exchange has queue bindings the broker does not delete it but close the channel with an exception instead.
// Set this to true if you are not the sole owner of the exchange.
//
// When noWait is true, do not wait for a broker confirmation that the exchange has been deleted.
// Failing to delete the channel could close the channel. Add a NotifyClose listener to respond to these channel exceptions.
func (c *Connection) RemoveExchange(name string, ifUnused, noWait bool) error {
	const errMessage = "failed to remove exchange: %w"

	if err := c.amqpChannel.ExchangeDelete(name, ifUnused, noWait); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

// DecodeDeliveryBody can be used to decode the body of a delivery into v.
func (c *Connection) DecodeDeliveryBody(delivery Delivery, v any) error {
	const errMessage = "failed to decode delivery body: %w"

	if err := c.options.codec.Decoder(delivery.Body, v); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (c *Connection) connect() error {
	const errMessage = "failed to connect to broker: %w"

	if c.amqpConnection == nil {
		if err := c.backOff(
			func() error {
				return c.createConnection()
			},
		); err != nil {
			return fmt.Errorf(errMessage, err)
		}

		if err := c.createChannel(); err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	return nil
}

func (c *Connection) createConnection() error {
	const errMessage = "failed to create channel: %w"

	var err error

	c.amqpConnMU.Lock()
	c.options.uriMU.RLock()
	c.amqpConnection, err = amqp.DialConfig(c.options.uri, amqp.Config(*c.options.Config))
	c.options.uriMU.RUnlock()
	c.amqpConnMU.Unlock()

	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.watchConnectionNotifications()

	return nil
}

func (c *Connection) createChannel() error {
	const errMessage = "failed to create channel: %w"

	var err error

	c.amqpConnMU.Lock()
	if c.amqpConnection == nil || c.amqpConnection.IsClosed() {
		c.amqpConnMU.Unlock()

		return ErrNoActiveConnection
	}

	c.amqpChanMU.Lock()

	c.amqpChannel, err = c.amqpConnection.Channel()

	c.amqpChanMU.Unlock()
	c.amqpConnMU.Unlock()

	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if c.options.PrefetchCount > 0 {
		if err = c.amqpChannel.Qos(c.options.PrefetchCount, 0, false); err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	c.watchChannelNotifications()

	return nil
}

type channelExec func(func(*amqp.Channel) error) error

func (c *Connection) channelExec(fn func(*amqp.Channel) error) error {
	c.amqpChanMU.Lock()
	defer c.amqpChanMU.Unlock()

	return fn(c.amqpChannel)
}

func (c *Connection) watchConnectionNotifications() {
	closeChan := c.amqpConnection.NotifyClose(make(chan *amqp.Error))
	blockChan := c.amqpConnection.NotifyBlocked(make(chan amqp.Blocking))

	go func() {
		for {
			select {
			case _, ok := <-closeChan:
				if !ok {
					c.logger.logDebug(context.Background(), "connection gracefully closed")

					c.connectionCloseWG.Done()

					return
				}

				c.handleClosedConnection()

				return

			case block := <-blockChan:
				c.logger.logWarn(context.Background(), "connection exception", "cause", block.Reason)
			}
		}
	}()
}

func (c *Connection) watchChannelNotifications() {
	closeChan := c.amqpChannel.NotifyClose(make(chan *amqp.Error))
	cancelChan := c.amqpChannel.NotifyCancel(make(chan string))
	returnChan := c.amqpChannel.NotifyReturn(make(chan amqp.Return))

	logCtx := context.Background()

	go func() {
		for {
			select {
			case err, ok := <-closeChan:
				if !ok {
					c.logger.logDebug(logCtx, "channel gracefully closed")

					c.connectionCloseWG.Done()

					return
				}

				c.handleClosedChannel(err)

				return

			case tag := <-cancelChan:
				c.logger.logWarn(logCtx, "cancel exception", "cause", tag)

			case rtn := <-returnChan:
				if c.returnHandler != nil {
					c.returnHandler(Return(rtn))

					continue
				}

				c.logger.logWarn(
					logCtx,
					"message could not be published",
					"replyCode", rtn.ReplyCode,
					"replyText", rtn.ReplyText,
					"messageId", rtn.MessageId,
					"correlationId", rtn.CorrelationId,
					"exchange", rtn.Exchange,
					"routingKey", rtn.RoutingKey,
				)
			}
		}
	}()
}

func (c *Connection) handleClosedConnection() {
	if err := c.recoverConnection(); err != nil {
		c.errChanMU.Lock()
		c.errChan <- &RecoveryFailedError{err, c.Name()}
		c.errChanMU.Unlock()
	}
}

func (c *Connection) handleClosedChannel(err *amqp.Error) {
	c.logger.logDebug(context.Background(), "channel unexpectedly closed", "cause", err)

	c.errChanMU.Lock()
	c.errChan <- err
	c.errChanMU.Unlock()

	if err := c.recoverChannel(); err != nil {
		c.errChanMU.Lock()
		c.errChan <- &RecoveryFailedError{err, c.Name()}
		c.errChanMU.Unlock()
	}
}

func (c *Connection) recoverConnection() error {
	const errMessage = "failed to recover connection: %w"

	c.amqpConnMU.Lock()
	c.amqpConnection = nil
	c.amqpConnMU.Unlock()

	if err := c.backOff(
		func() error {
			return c.createConnection()
		},
	); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.logger.logDebug(context.Background(), "successfully recovered connection")

	return nil
}

func (c *Connection) recoverChannel() error {
	const errMessage = "failed to recover channel: %w"

	c.amqpChanMU.Lock()
	c.amqpChannel = nil
	c.amqpChanMU.Unlock()

	if err := c.backOff(
		func() error {
			return c.createChannel()
		},
	); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if len(c.consumerRecoverFNs) > 0 {
		if err := c.recoverConsumers(); err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	c.recoverPublishers()

	c.logger.logDebug(context.Background(), "successfully recovered channel")

	return nil
}

func (c *Connection) recoverConsumers() error {
	const errMessage = "failed to recover consumers: %w"

	c.consumerRecoverFNsMtx.RLock()
	defer c.consumerRecoverFNsMtx.RUnlock()

	for consumerTag := range c.consumerRecoverFNs {
		if err := c.consumerRecoverFNs[consumerTag](); err != nil {
			return fmt.Errorf(errMessage, err)
		}

		c.logger.logDebug(context.Background(), "successfully recovered consumer", "consumerTag", consumerTag)
	}

	c.logger.logDebug(context.Background(), "successfully recovered consumers")

	return nil
}

func (c *Connection) addConsumerRecoveryFN(consumerTag string, fn func() error) {
	c.consumerRecoverFNsMtx.Lock()
	defer c.consumerRecoverFNsMtx.Unlock()

	c.consumerRecoverFNs[consumerTag] = fn
}

func (c *Connection) removeConsumerRecoveryFN(consumerTag string) {
	c.consumerRecoverFNsMtx.Lock()
	defer c.consumerRecoverFNsMtx.Unlock()

	delete(c.consumerRecoverFNs, consumerTag)
}

func (c *Connection) cancelConsumer(consumerTag string) error {
	const errMessage = "failed to cancel consumer: %w"

	if c.isClosed() {
		return fmt.Errorf(errMessage, amqp.ErrClosed)
	}

	c.amqpChanMU.Lock()
	defer c.amqpChanMU.Unlock()

	if err := c.amqpChannel.Cancel(consumerTag, false); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.removeConsumerRecoveryFN(consumerTag)

	return nil
}

func (c *Connection) recoverPublishers() {
	c.publisherCheckCacheFNsMtx.RLock()
	defer c.publisherCheckCacheFNsMtx.RUnlock()

	for publisherName := range c.publisherCheckCacheFNs {
		c.publisherCheckCacheFNs[publisherName]()
	}
}

func (c *Connection) addPublisherCheckCacheFN(publisherName string, fn func()) {
	c.publisherCheckCacheFNsMtx.Lock()
	defer c.publisherCheckCacheFNsMtx.Unlock()

	c.publisherCheckCacheFNs[publisherName] = fn
}

func (c *Connection) removePublisherCheckCacheFN(publisherName string) {
	c.publisherCheckCacheFNsMtx.Lock()
	defer c.publisherCheckCacheFNsMtx.Unlock()

	delete(c.publisherCheckCacheFNs, publisherName)
}

func (c *Connection) backOff(action func() error) error {
	const errMessage = "backOff failed: %w"

	retry := 0

	for retry <= c.options.MaxRecoveryRetries {
		logCtx := context.Background()

		if action() == nil {
			break
		}

		if retry == c.options.MaxRecoveryRetries {
			c.logger.logDebug(logCtx, "recovery failed: maximum retries exceeded", "retries", retry)

			return fmt.Errorf(errMessage, ErrMaxRetriesExceeded)
		}

		delay := time.Duration(c.options.BackOffFactor*retry) * c.options.RecoveryInterval

		c.logger.logDebug(logCtx, "failed to recover: backing off...", "backOffTime", delay.String())

		time.Sleep(delay)

		retry++
	}

	return nil
}

func (c *Connection) isClosed() bool {
	c.amqpChanMU.Lock()
	defer c.amqpChanMU.Unlock()

	return c.amqpChannel == nil || c.amqpChannel.IsClosed()
}
