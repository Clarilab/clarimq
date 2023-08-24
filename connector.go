package gorabbitmq

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	publish string = "publish"
	consume string = "consume"

	closeWGDelta int = 2

	reconnectFailChanSize int = 32
)

// Connector manages the connection to a RabbitMQ cluster.
type Connector struct {
	options *ConnectorOptions
	log     *log

	reconnectFailChanMtx *sync.Mutex
	reconnectFailChan    chan error

	publishConnection *connection
	consumeConnection *connection
}

// NewConnector creates a new Connector instance.
//
// Needs to be closed with the Close() method.
func NewConnector(settings *ConnectionSettings, options ...ConnectorOption) *Connector {
	opt := defaultConnectorOptions(
		fmt.Sprintf("amqp://%s:%s@%s/",
			url.QueryEscape(settings.UserName),
			url.QueryEscape(settings.Password),
			net.JoinHostPort(
				url.QueryEscape(settings.Host),
				strconv.Itoa(settings.Port),
			),
		),
	)

	for i := 0; i < len(options); i++ {
		options[i](opt)
	}

	return &Connector{
		options:              opt,
		log:                  newLogger(opt.logger),
		reconnectFailChanMtx: &sync.Mutex{},
		reconnectFailChan:    make(chan error, reconnectFailChanSize),
	}
}

// Close gracefully closes the connection to the server.
func (c *Connector) Close() error {
	const errMessage = "failed to close connections to rabbitmq gracefully: %w"

	if c.publishConnection != nil &&
		c.publishConnection.amqpConnection != nil {
		c.log.logDebug("closing connection", "type", publish)

		c.publishConnection.connectionCloseWG.Add(closeWGDelta)

		c.publishConnection.amqpConnectionMtx.Lock()
		err := c.publishConnection.amqpConnection.Close()
		c.publishConnection.amqpConnectionMtx.Unlock()

		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		c.publishConnection.connectionCloseWG.Wait()

		c.publishConnection.reconnectFailChanMtx.Lock()
		close(c.publishConnection.reconnectFailChan)
		c.publishConnection.reconnectFailChanMtx.Unlock()

		close(c.publishConnection.reconnectChan)
	}

	if c.consumeConnection != nil &&
		c.consumeConnection.amqpConnection != nil {
		c.log.logDebug("closing connection", "type", consume)

		c.consumeConnection.connectionCloseWG.Add(closeWGDelta)

		c.consumeConnection.amqpConnectionMtx.Lock()
		err := c.consumeConnection.amqpConnection.Close()
		c.consumeConnection.amqpConnectionMtx.Unlock()

		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		c.consumeConnection.connectionCloseWG.Wait()

		c.consumeConnection.reconnectFailChanMtx.Lock()
		close(c.consumeConnection.reconnectFailChan)
		c.consumeConnection.reconnectFailChanMtx.Unlock()

		close(c.consumeConnection.reconnectChan)
		close(c.consumeConnection.consumerCloseChan)
	}

	c.reconnectFailChanMtx.Lock()
	close(c.reconnectFailChan)
	c.reconnectFailChanMtx.Unlock()

	c.log.logInfo("gracefully closed connections to rabbitmq")

	return nil
}

// DecodeDeliveryBody can be used to decode the body of a delivery into v.
func (c *Connector) DecodeDeliveryBody(delivery Delivery, v any) error {
	const errMessage = "failed to decode delivery body: %w"

	if err := c.options.Codec.Decoder(delivery.Body, v); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

// NotifyFailedRecovery returns a channel that delivers an error if the reconnection to RabbitMQ
// failed by exeeding the maximum number of retries.
//
// Should be used e.g. to then close your application gracefully, or manually trying a reconnect again.
func (c *Connector) NotifyFailedRecovery() (<-chan error, error) {
	const errMessage = "failed to notify failed recovery: %w"

	if c.publishConnection == nil && c.consumeConnection == nil {
		return nil, fmt.Errorf(errMessage, ErrNoActiveConnection)
	}

	c.watchReconnectionFailes()

	return c.reconnectFailChan, nil
}

// Reconnect can be used to manually reconnect to a RabbitMQ.
//
// Returns an error if the current connection persists.
func (c *Connector) Reconnect() error {
	const errMessage = "failed to reconnect to rabbitmq: %w"

	c.publishConnection.amqpChannelMtx.Lock()
	c.consumeConnection.amqpChannelMtx.Lock()
	if c.publishConnection.amqpConnection != nil && c.consumeConnection.amqpConnection != nil {
		c.publishConnection.amqpChannelMtx.Unlock()
		c.consumeConnection.amqpChannelMtx.Unlock()

		return fmt.Errorf(errMessage, ErrHealthyConnection)
	}

	c.publishConnection.amqpChannelMtx.Unlock()
	c.consumeConnection.amqpChannelMtx.Unlock()

	c.watchReconnectionFailes()

	err := c.publishConnection.reconnect(publish, c.options, c.log)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	err = c.consumeConnection.reconnect(consume, c.options, c.log)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (c *Connector) watchReconnectionFailes() {
	go func() {
		for {
			select {
			case err := <-c.publishConnection.reconnectFailChan:
				c.reconnectFailChanMtx.Lock()
				c.reconnectFailChan <- err
				c.reconnectFailChanMtx.Unlock()

				return
			case err := <-c.consumeConnection.reconnectFailChan:
				c.reconnectFailChanMtx.Lock()
				c.reconnectFailChan <- err
				c.reconnectFailChanMtx.Unlock()

				return
			}
		}
	}()
}

func connect(conn *connection, opt *ConnectorOptions, logger *log, instanceType string) error {
	const errMessage = "failed to connect to rabbitmq: %w"

	if conn.amqpConnection == nil {
		err := createConnection(conn, opt, instanceType, logger)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		err = createChannel(conn, opt, instanceType, logger)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		conn.reconnectChan = make(chan struct{})

		conn.watchReconnects(instanceType, opt, logger)
		conn.watchConsumerClose()
	}

	return nil
}

func createConnection(conn *connection, opt *ConnectorOptions, instanceType string, logger *log) error {
	const errMessage = "failed to create channel: %w"

	var err error

	conn.amqpConnection, err = amqp.DialConfig(opt.uri, amqp.Config(*opt.Config))
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	watchConnectionNotifications(conn, instanceType, logger)

	return nil
}

func createChannel(conn *connection, opt *ConnectorOptions, instanceType string, logger *log) error {
	const errMessage = "failed to create channel: %w"

	var err error

	conn.amqpChannel, err = conn.amqpConnection.Channel()
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if opt.PrefetchCount > 0 {
		err = conn.amqpChannel.Qos(opt.PrefetchCount, 0, false)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	watchChannelNotifications(conn, instanceType, opt.ReturnHandler, logger)

	return nil
}

func watchConnectionNotifications(conn *connection, instanceType string, logger *log) {
	closeChan := conn.amqpConnection.NotifyClose(make(chan *amqp.Error))
	blockChan := conn.amqpConnection.NotifyBlocked(make(chan amqp.Blocking))

	go func() {
		for {
			select {
			case err := <-closeChan:
				if err == nil {
					slog.Debug("closed connection", "type", instanceType)

					conn.connectionCloseWG.Done()

					return
				}

				logger.logDebug("connection unexpectedly closed", "type", instanceType, "cause", err)

				conn.reconnectChan <- struct{}{}

				return

			case block := <-blockChan:
				logger.logWarn("connection exception", "type", instanceType, "cause", block.Reason)
			}
		}
	}()
}

func watchChannelNotifications(conn *connection, instanceType string, returnHandler ReturnHandler, logger *log) {
	closeChan := conn.amqpChannel.NotifyClose(make(chan *amqp.Error))
	cancelChan := conn.amqpChannel.NotifyCancel(make(chan string))
	returnChan := conn.amqpChannel.NotifyReturn(make(chan amqp.Return))

	go func() {
		for {
			select {
			case err := <-closeChan:
				if err == nil {
					slog.Debug("closed channel", "type", instanceType)

					conn.connectionCloseWG.Done()

					return
				}

				logger.logDebug("channel unexpectedly closed", "type", instanceType, "cause", err)

				return

			case tag := <-cancelChan:
				logger.logWarn("cancel exception", "type", instanceType, "cause", tag)

			case rtrn := <-returnChan:
				if returnHandler != nil {
					returnHandler(Return(rtrn))

					continue
				}

				logger.logWarn(
					"message could not be published",
					"replyCode", rtrn.ReplyCode,
					"replyText", rtrn.ReplyText,
					"messageId", rtrn.MessageId,
					"correlationId", rtrn.CorrelationId,
					"exchange", rtrn.Exchange,
					"routingKey", rtrn.RoutingKey,
				)
			}
		}
	}()
}
