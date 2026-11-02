package clarimq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	errChanSize     int           = 32
	maxBackOffDelay time.Duration = 5 * time.Minute
)

type Connection struct {
	options *ConnectionOptions

	amqpConnMU     sync.RWMutex
	amqpConnection *amqp.Connection

	amqpChanMU  sync.RWMutex
	amqpChannel *amqp.Channel

	// tracks all running notification watchers, so closing can wait for them to finish
	watcherWG sync.WaitGroup

	// serializes the automatic recovery, Recover and Renew, so they never replace the connection concurrently
	recoveryMU sync.Mutex
	// wakes up a running back-off, so it retries immediately (e.g. with the new URI passed to Renew)
	wakeUp chan struct{}

	closed atomic.Bool
	done   chan struct{}

	errChanMU     sync.Mutex
	errChan       chan error
	errChanClosed bool

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

	if opt.Config != nil && opt.Config.Recovery != nil {
		return nil, fmt.Errorf(errMessage, ErrAMQPRecoveryNotSupported)
	}

	conn := &Connection{
		done:                   make(chan struct{}),
		wakeUp:                 make(chan struct{}, 1),
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
	return settings.ToURI()
}

// Close gracefully closes the connection to the broker.
//
// A running recovery is aborted. Calling Close more than once has no effect.
func (c *Connection) Close() error {
	const errMessage = "failed to close the connection to the broker gracefully: %w"

	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(c.done)

	logCtx := context.Background()

	c.logger.logDebug(logCtx, "closing connection")

	if err := c.closeAMQPConnection(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.watcherWG.Wait()

	c.closeErrChan()

	c.logger.logInfo(logCtx, "gracefully closed connection to the broker")

	return nil
}

// IsClosed reports if the connection to rabbitmq is currently closed.
func (c *Connection) IsClosed() bool {
	c.amqpChanMU.RLock()
	defer c.amqpChanMU.RUnlock()

	return c.amqpChannel == nil || c.amqpChannel.IsClosed()
}

// Name returns the name of the connection if specified, otherwise returns an empty string.
func (c *Connection) Name() string {
	if name, ok := c.options.Config.Properties[amqpConnectionNameKey].(string); ok {
		return name
	}

	return ""
}

// NotifyErrors returns a channel that will return an errors that happen concurrently.
//
// The channel is buffered. When it is full, further errors are logged and dropped,
// so an undrained channel never blocks the recovery.
func (c *Connection) NotifyErrors() <-chan error {
	return c.errChan
}

// Recover can be used to manually start the recovery.
//
// Only the broken parts are recovered: if the connection is still open, only the channel is recovered.
// If neither is broken, ErrHealthyConnection is returned.
func (c *Connection) Recover() error {
	const errMessage = "failed to recover: %w"

	c.recoveryMU.Lock()
	defer c.recoveryMU.Unlock()

	connectionOpen := c.isConnectionOpen()

	if connectionOpen && !c.IsClosed() {
		return fmt.Errorf(errMessage, ErrHealthyConnection)
	}

	if !connectionOpen {
		if err := c.recoverConnection(); err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	if err := c.recoverChannel(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

// Renew can be used to establish a new connection.
// If new URI is provided, it will be used to renew the connection instead of the current URI.
//
// If a recovery is running, it retries immediately and Renew waits for it to finish before renewing.
func (c *Connection) Renew(uri ...string) error {
	const errMessage = "failed to renew: %w"

	if len(uri) == 1 {
		c.options.uriMU.Lock()
		c.options.uri = uri[0]
		c.options.uriMU.Unlock()
	}

	c.wakeUpBackOff()

	c.recoveryMU.Lock()
	defer c.recoveryMU.Unlock()

	// recoverConnection closes the current connection before establishing the new one
	if err := c.recoverConnection(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if err := c.recoverChannel(); err != nil {
		return fmt.Errorf(errMessage, err)
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

	var purgedMessages int

	if err := c.channelExec(func(channel *amqp.Channel) error {
		var err error

		purgedMessages, err = channel.QueueDelete(name, ifUnused, ifEmpty, noWait)

		return err //nolint:wrapcheck // intended
	}); err != nil {
		return 0, fmt.Errorf(errMessage, err)
	}

	return purgedMessages, nil
}

func (c *Connection) InspectQueue(name string) (*QueueInfo, error) {
	const errMessage = "failed to inspect queue: %w"

	result, err := getQueueInfo(c.channelExec, name)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	return result, nil
}

// RemoveBinding removes a binding between an exchange and queue matching the key and arguments.
//
// It is possible to send and empty string for the exchange name which means to unbind the queue from the default exchange.
func (c *Connection) RemoveBinding(queueName, routingKey, exchangeName string, args Table) error {
	const errMessage = "failed to remove binding: %w"

	if err := c.channelExec(func(channel *amqp.Channel) error {
		return channel.QueueUnbind(queueName, routingKey, exchangeName, amqp.Table(args))
	}); err != nil {
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

	if err := c.channelExec(func(channel *amqp.Channel) error {
		return channel.ExchangeDelete(name, ifUnused, noWait)
	}); err != nil {
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

func (c *Connection) isConnectionOpen() bool {
	c.amqpConnMU.RLock()
	defer c.amqpConnMU.RUnlock()

	return c.amqpConnection != nil && !c.amqpConnection.IsClosed()
}

func (c *Connection) closeAMQPConnection() error {
	c.amqpConnMU.RLock()
	conn := c.amqpConnection
	c.amqpConnMU.RUnlock()

	if conn == nil || conn.IsClosed() {
		return nil
	}

	if err := conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		return err //nolint:wrapcheck // wrapped by the caller
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
	const errMessage = "failed to create connection: %w"

	c.amqpConnMU.Lock()
	defer c.amqpConnMU.Unlock()

	// checked while holding the lock: Close either prevents the dial or sees and closes the new connection
	if c.closed.Load() {
		return fmt.Errorf(errMessage, ErrConnectionClosed)
	}

	c.options.uriMU.RLock()
	conn, err := amqp.DialConfig(c.options.uri, amqp.Config(*c.options.Config))
	c.options.uriMU.RUnlock()

	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.amqpConnection = conn

	c.watchConnectionNotifications(conn)

	return nil
}

func (c *Connection) createChannel() error {
	const errMessage = "failed to create channel: %w"

	c.amqpConnMU.RLock()
	defer c.amqpConnMU.RUnlock()

	if c.amqpConnection == nil || c.amqpConnection.IsClosed() {
		return ErrNoActiveConnection
	}

	c.amqpChanMU.Lock()
	defer c.amqpChanMU.Unlock()

	channel, err := c.amqpConnection.Channel()
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if c.options.PrefetchCount > 0 {
		if err = channel.Qos(c.options.PrefetchCount, 0, false); err != nil {
			return fmt.Errorf(errMessage, errors.Join(err, channel.Close()))
		}
	}

	if c.options.PublisherConfirms {
		if err = channel.Confirm(false); err != nil {
			return fmt.Errorf(errMessage, errors.Join(err, channel.Close()))
		}
	}

	c.amqpChannel = channel

	c.watchChannelNotifications(c.amqpConnection, channel)

	return nil
}

type channelExec func(func(*amqp.Channel) error) error

func (c *Connection) channelExec(fn func(*amqp.Channel) error) error {
	// exclusive lock on purpose: amqp091 does not serialize synchronous calls on a channel,
	// so concurrent calls could receive each other's responses.
	c.amqpChanMU.Lock()
	defer c.amqpChanMU.Unlock()

	// the channel is nil while it is being recovered
	if c.amqpChannel == nil {
		return amqp.ErrClosed
	}

	return fn(c.amqpChannel)
}

// temporaryChannelExec executes fn on a dedicated short-lived channel,
// so that a channel exception raised by fn does not close the shared channel.
func (c *Connection) temporaryChannelExec(fn func(*amqp.Channel) error) error {
	const errMessage = "failed to execute on temporary channel: %w"

	c.amqpConnMU.RLock()

	if c.amqpConnection == nil || c.amqpConnection.IsClosed() {
		c.amqpConnMU.RUnlock()

		return ErrNoActiveConnection
	}

	channel, err := c.amqpConnection.Channel()

	c.amqpConnMU.RUnlock()

	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	err = fn(channel)

	if !channel.IsClosed() {
		err = errors.Join(err, channel.Close())
	}

	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (c *Connection) watchConnectionNotifications(conn *amqp.Connection) {
	closeChan := conn.NotifyClose(make(chan *amqp.Error, 1))
	blockChan := conn.NotifyBlocked(make(chan amqp.Blocking))

	c.watcherWG.Go(func() {
		for {
			select {
			case err, ok := <-closeChan:
				if !ok {
					c.logger.logDebug(context.Background(), "connection gracefully closed")

					return
				}

				c.logger.logDebug(context.Background(), "connection unexpectedly closed", errAttr(err))

				c.handleClosedConnection(conn)

				return

			case block := <-blockChan:
				c.logger.logWarn(context.Background(), "connection exception", "cause", block.Reason)
			}
		}
	})
}

func (c *Connection) watchChannelNotifications(conn *amqp.Connection, channel *amqp.Channel) {
	closeChan := channel.NotifyClose(make(chan *amqp.Error, 1))
	cancelChan := channel.NotifyCancel(make(chan string))
	returnChan := channel.NotifyReturn(make(chan amqp.Return))

	logCtx := context.Background()

	c.watcherWG.Go(func() {
		for {
			select {
			case err, ok := <-closeChan:
				if !ok {
					c.logger.logDebug(logCtx, "channel gracefully closed")

					return
				}

				c.handleClosedChannel(err, conn, channel)

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
	})
}

// handleClosedConnection recovers the lost connection and its channel.
func (c *Connection) handleClosedConnection(conn *amqp.Connection) {
	c.recoveryMU.Lock()
	defer c.recoveryMU.Unlock()

	// the connection was already replaced by Recover or Renew
	if !c.isCurrentConnection(conn) {
		return
	}

	if err := c.recoverConnection(); err != nil {
		c.notifyRecoveryFailed(err)

		return
	}

	if err := c.recoverChannel(); err != nil {
		c.notifyRecoveryFailed(err)
	}
}

// handleClosedChannel recovers a channel that was closed while its connection stays open.
func (c *Connection) handleClosedChannel(err *amqp.Error, conn *amqp.Connection, channel *amqp.Channel) {
	c.logger.logDebug(context.Background(), "channel unexpectedly closed", "cause", err)

	c.notifyError(err)

	// on a connection loss, the connection watcher recovers the connection and the channel
	if conn.IsClosed() {
		return
	}

	c.recoveryMU.Lock()
	defer c.recoveryMU.Unlock()

	// the channel was already replaced by Recover or Renew
	if !c.isCurrentChannel(channel) {
		return
	}

	if err := c.recoverChannel(); err != nil {
		c.notifyRecoveryFailed(err)
	}
}

func (c *Connection) notifyRecoveryFailed(err error) {
	// closing aborts the recovery on purpose, and a lost connection is recovered by its own watcher
	if errors.Is(err, ErrConnectionClosed) || errors.Is(err, ErrNoActiveConnection) {
		return
	}

	c.notifyError(&RecoveryFailedError{err, c.Name()})
}

func (c *Connection) isCurrentConnection(conn *amqp.Connection) bool {
	c.amqpConnMU.RLock()
	defer c.amqpConnMU.RUnlock()

	return c.amqpConnection == conn
}

func (c *Connection) isCurrentChannel(channel *amqp.Channel) bool {
	c.amqpChanMU.RLock()
	defer c.amqpChanMU.RUnlock()

	return c.amqpChannel == channel
}

// notifyError passes err to the error channel without blocking.
// If the channel is full or already closed, the error is logged and dropped.
func (c *Connection) notifyError(err error) {
	c.errChanMU.Lock()
	defer c.errChanMU.Unlock()

	if c.errChanClosed {
		c.logger.logError(context.Background(), "error channel is closed, dropping error", err)

		return
	}

	select {
	case c.errChan <- err:
	default:
		c.logger.logError(context.Background(), "error channel is full, dropping error", err)
	}
}

func (c *Connection) closeErrChan() {
	c.errChanMU.Lock()
	defer c.errChanMU.Unlock()

	if !c.errChanClosed {
		c.errChanClosed = true

		close(c.errChan)
	}
}

func (c *Connection) recoverConnection() error {
	const errMessage = "failed to recover connection: %w"

	c.amqpConnMU.Lock()
	previousConnection := c.amqpConnection
	c.amqpConnection = nil
	c.amqpConnMU.Unlock()

	// a still open connection is closed, so that it and its watchers do not leak
	if previousConnection != nil && !previousConnection.IsClosed() {
		if err := previousConnection.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			c.logger.logError(context.Background(), "failed to close previous connection", err)
		}
	}

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
	previousChannel := c.amqpChannel
	c.amqpChannel = nil
	c.amqpChanMU.Unlock()

	// a still open channel is closed, so that its watcher and consumers do not keep running
	if previousChannel != nil && !previousChannel.IsClosed() {
		if err := previousChannel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			c.logger.logError(context.Background(), "failed to close previous channel", err)
		}
	}

	if err := c.backOff(
		func() error {
			return c.createChannel()
		},
	); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if err := c.recoverConsumers(); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.recoverPublishers()

	c.logger.logDebug(context.Background(), "successfully recovered channel")

	return nil
}

// recoverConsumers recovers all registered consumers.
// A failing consumer does not prevent the recovery of the others.
func (c *Connection) recoverConsumers() error {
	const errMessage = "failed to recover consumers: %w"

	c.consumerRecoverFNsMtx.RLock()
	defer c.consumerRecoverFNsMtx.RUnlock()

	var errs []error

	for consumerTag, recoverFN := range c.consumerRecoverFNs {
		if err := recoverFN(); err != nil {
			errs = append(errs, fmt.Errorf("consumer %q: %w", consumerTag, err))

			continue
		}

		c.logger.logDebug(context.Background(), "successfully recovered consumer", "consumerTag", consumerTag)
	}

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	c.logger.logDebug(context.Background(), "successfully recovered consumers")

	return nil
}

func (c *Connection) addConsumerRecoveryFN(consumerTag string, fn func() error) error {
	c.consumerRecoverFNsMtx.Lock()
	defer c.consumerRecoverFNsMtx.Unlock()

	if _, ok := c.consumerRecoverFNs[consumerTag]; ok {
		return ErrConsumerAlreadyExists
	}

	c.consumerRecoverFNs[consumerTag] = fn

	return nil
}

func (c *Connection) removeConsumerRecoveryFN(consumerTag string) {
	c.consumerRecoverFNsMtx.Lock()
	defer c.consumerRecoverFNsMtx.Unlock()

	delete(c.consumerRecoverFNs, consumerTag)
}

func (c *Connection) cancelConsumer(consumerTag string) error {
	const errMessage = "failed to cancel consumer: %w"

	if err := c.channelExec(func(channel *amqp.Channel) error {
		return channel.Cancel(consumerTag, false)
	}); err != nil {
		return fmt.Errorf(errMessage, err)
	}

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

// backOff retries action with an exponentially growing delay, capped at maxBackOffDelay.
// A negative MaxRecoveryRetries retries indefinitely. Closing the connection aborts the back-off.
func (c *Connection) backOff(action func() error) error {
	const errMessage = "backOff failed: %w"

	logCtx := context.Background()

	delay := c.options.RecoveryInterval

	// discard a wake-up that was not consumed by a previous back-off
	select {
	case <-c.wakeUp:
	default:
	}

	for retry := 0; ; retry++ {
		if c.closed.Load() {
			return fmt.Errorf(errMessage, ErrConnectionClosed)
		}

		err := action()
		if err == nil {
			return nil
		}

		// retrying without a connection is pointless, the connection recovery also recovers the channel
		if errors.Is(err, ErrNoActiveConnection) {
			return fmt.Errorf(errMessage, err)
		}

		if c.options.MaxRecoveryRetries >= 0 && retry >= c.options.MaxRecoveryRetries {
			c.logger.logDebug(logCtx, "recovery failed: maximum retries exceeded", "retries", retry, "error", err)

			return fmt.Errorf(errMessage, ErrMaxRetriesExceeded)
		}

		c.logger.logDebug(logCtx, "failed to recover: backing off...", "backOffTime", delay.String(), "error", err)

		select {
		case <-time.After(delay):
		case <-c.wakeUp:
		case <-c.done:
			return fmt.Errorf(errMessage, ErrConnectionClosed)
		}

		delay = min(delay*time.Duration(max(c.options.BackOffFactor, 1)), maxBackOffDelay)
	}
}

// wakeUpBackOff makes a running back-off retry immediately.
func (c *Connection) wakeUpBackOff() {
	select {
	case c.wakeUp <- struct{}{}:
	default:
	}
}
