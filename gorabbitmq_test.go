package gorabbitmq_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Clarilab/gorabbitmq/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	logLevel = slog.LevelError
)

type testData struct {
	Name    string `json:"name"`
	Age     int    `json:"age"`
	City    string `json:"city"`
	Country string `json:"country"`
}

func Test_Integration_PublishToExchange(t *testing.T) { //nolint:maintidx // table testing
	t.Parallel()

	stringMessage := "test-message"
	bytesMessage := []byte(stringMessage)

	jsonMessage := testData{
		Name:    "Name",
		Age:     157,
		City:    "City",
		Country: "Country",
	}

	type testParams struct {
		exchangeName string
		queueName    string
		routingKey   string
	}

	tests := map[string]struct {
		testParams      *testParams
		connector       *gorabbitmq.Connector
		deliveryHandler func(any, int, chan struct{}) gorabbitmq.HandlerFunc
		preConsumer     func(*gorabbitmq.Connector, gorabbitmq.HandlerFunc, *testParams) (*gorabbitmq.Consumer, error)
		getConsumer     func(*gorabbitmq.Connector, gorabbitmq.HandlerFunc, *testParams) (*gorabbitmq.Consumer, error)
		getPublisher    func(*gorabbitmq.Connector, *testParams) (*gorabbitmq.Publisher, error)
		publish         func(*testParams) error
		message         any
		doneChan        chan struct{}
	}{
		"publish to exchange / consume with Ack": {
			testParams: &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			},
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				),
					gorabbitmq.WithConnectorOptionConnectionName("test-connection"),
				)
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithExchangeOptionArgs(gorabbitmq.Table{
						"test-exchange-arg-key": "test-exchange-arg-value",
					}),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionExchange(params.exchangeName),
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			message:  stringMessage,
			doneChan: make(chan struct{}),
		},
		"publish to exchange / consume with NackDisgard": {
			testParams: &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			},
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.NackDiscard
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionExchange(params.exchangeName),
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			message:  stringMessage,
			doneChan: make(chan struct{}),
		},
		"publish to exchange / consume with NackRequeue": {
			testParams: &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			},
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)
					counter++

					switch counter {
					case 1:
						return gorabbitmq.NackRequeue

					case 2:
						doneChan <- struct{}{}

						return gorabbitmq.Ack
					}

					return gorabbitmq.NackDiscard
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionExchange(params.exchangeName),
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			message:  stringMessage,
			doneChan: make(chan struct{}),
		},
		"publish to exchange / consume with manual Ack": {
			testParams: &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			},
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				),
					gorabbitmq.WithConnectorOptionConnectionName("test-connection"),
				)
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(delivery gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(delivery.Body))
					requireEqual(t, "text/plain", delivery.ContentType)

					doneChan <- struct{}{}

					err := delivery.Ack(false)
					requireNoError(t, err)

					return gorabbitmq.Manual
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithExchangeOptionArgs(gorabbitmq.Table{
						"test-exchange-arg-key": "test-exchange-arg-value",
					}),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionExchange(params.exchangeName),
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			message:  stringMessage,
			doneChan: make(chan struct{}),
		},
		"publish to exchange / consume with AutoAck": {
			testParams: &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			},
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Manual
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
					gorabbitmq.WithConsumerOptionConsumerAutoAck(true),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionExchange(params.exchangeName),
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			message:  stringMessage,
			doneChan: make(chan struct{}),
		},
		"publish to exchange / consume with exchange NoWait": {
			testParams: &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			},
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithExchangeOptionNoWait(true),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionExchange(params.exchangeName),
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			message:  stringMessage,
			doneChan: make(chan struct{}),
		},
		"publish to exchange / consume with consumer NoWait": {
			testParams: &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			},
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionNoWait(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionExchange(params.exchangeName),
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			message:  stringMessage,
			doneChan: make(chan struct{}),
		},
		"publish to exchange / consume with multiple message handlers": {
			testParams: &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			},
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionHandlerQuantity(4),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionExchange(params.exchangeName),
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			message:  stringMessage,
			doneChan: make(chan struct{}),
		},
		"publish to exchange passive": {
			testParams: &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			},
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, d.Body)
					requireEqual(t, "application/octet-stream", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			preConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
					gorabbitmq.WithExchangeOptionPassive(true),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionExchange(params.exchangeName),
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			message:  bytesMessage,
			doneChan: make(chan struct{}),
		},
		"publish bytes message": {
			testParams: &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			},
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, d.Body)
					requireEqual(t, "application/octet-stream", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionExchange(params.exchangeName),
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			message:  bytesMessage,
			doneChan: make(chan struct{}),
		},
		"publish json message": {
			testParams: &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			},
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, "application/json", d.ContentType)

					var result testData

					err := json.Unmarshal(d.Body, &result)
					requireNoError(t, err)

					requireEqual(t, expectedMessage, result)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionExchange(params.exchangeName),
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			message:  jsonMessage,
			doneChan: make(chan struct{}),
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			t.Cleanup(func() {
				err := test.connector.Close()
				requireNoError(t, err)
			})

			var counter int

			if test.preConsumer != nil {
				consumer, err := test.preConsumer(test.connector, nil, test.testParams)
				requireNoError(t, err)

				err = consumer.Unsubscribe()
				requireNoError(t, err)
			}

			consumer, err := test.getConsumer(test.connector, test.deliveryHandler(test.message, counter, test.doneChan), test.testParams)
			requireNoError(t, err)

			publisher, err := test.getPublisher(test.connector, test.testParams)
			requireNoError(t, err)

			err = publisher.Publish(context.TODO(), test.testParams.routingKey, test.message)
			requireNoError(t, err)

			<-test.doneChan

			if test.preConsumer != nil {
				err = consumer.RemoveExchange(test.testParams.exchangeName, false, false)
				requireNoError(t, err)
			}
		})
	}
}

func Test_Integration_PublishToQueue(t *testing.T) { //nolint:maintidx // table testing
	t.Parallel()

	message := "test-message"

	tests := map[string]struct {
		connector       *gorabbitmq.Connector
		deliveryHandler func(any, chan struct{}) gorabbitmq.HandlerFunc
		getConsumer     func(*gorabbitmq.Connector, gorabbitmq.HandlerFunc, string) (*gorabbitmq.Consumer, error)
		preConsumer     func(*gorabbitmq.Connector, gorabbitmq.HandlerFunc, string) (*gorabbitmq.Consumer, error)
		getPublisher    func(*gorabbitmq.Connector) (*gorabbitmq.Publisher, error)
		publish         func(*gorabbitmq.Publisher, string) error
		message         any
		doneChan        chan struct{}
		queueName       string
	}{
		"publish to queue": {
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionArgs(gorabbitmq.Table{
						"test-queue-arg-key": "test-queue-arg-value",
					}),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.Publish(context.TODO(), target, message)
			},
			doneChan:  make(chan struct{}),
			message:   message,
			queueName: stringGen(),
		},
		"publish to queue passive": {
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			preConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					queueName,
					handler,
				)
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionPassive(true),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.Publish(context.TODO(), target, message)
			},
			doneChan:  make(chan struct{}),
			message:   message,
			queueName: stringGen(),
		},
		"publish to queue NoWait": {
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionNoWait(true),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.Publish(context.TODO(), target, message)
			},
			doneChan:  make(chan struct{}),
			message:   message,
			queueName: stringGen(),
		},
		"publish to priority queue": {
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)
					requireEqual(t, 4, int(d.Priority))

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionPriority(gorabbitmq.HighestPublishingPriority),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionPriority(gorabbitmq.HighPublishingPriority),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.Publish(context.TODO(), target, message)
			},
			doneChan:  make(chan struct{}),
			message:   message,
			queueName: stringGen(),
		},
		"publish to durable queue": {
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionDurable(true),
				)
			},
			getPublisher: func(c *gorabbitmq.Connector) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionMandatory(true),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.Publish(context.TODO(), target, message)
			},
			doneChan:  make(chan struct{}),
			message:   message,
			queueName: stringGen(),
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			t.Cleanup(func() {
				err := test.connector.Close()
				requireNoError(t, err)
			})

			if test.preConsumer != nil {
				consumer, err := test.preConsumer(test.connector, nil, test.queueName)
				requireNoError(t, err)

				err = consumer.Unsubscribe()
				requireNoError(t, err)
			}

			consumer, err := test.getConsumer(test.connector, test.deliveryHandler(test.message, test.doneChan), test.queueName)
			requireNoError(t, err)

			publisher, err := test.getPublisher(test.connector)
			requireNoError(t, err)

			err = test.publish(publisher, test.queueName)
			requireNoError(t, err)

			<-test.doneChan

			if test.preConsumer != nil {
				_, err = consumer.RemoveQueue(test.queueName, false, false, false)
				requireNoError(t, err)
			}
		})
	}
}

func Test_Integration_PublishToMultiple(t *testing.T) {
	t.Parallel()

	message := "test-message"

	now := time.Date(2023, 8, 1, 12, 0, 0, 0, time.Local)

	tests := map[string]struct {
		connector       *gorabbitmq.Connector
		deliveryHandler func(any, *sync.WaitGroup) gorabbitmq.HandlerFunc
		getPublisher    func(*gorabbitmq.Connector) (*gorabbitmq.Publisher, error)
		publish         func(*gorabbitmq.Publisher, []string) error
		targets         []string
		message         any
	}{
		"publish to multiple": {
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, wg *sync.WaitGroup) gorabbitmq.HandlerFunc {
				return func(delivery gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(delivery.Body))
					requireEqual(t, "test-service", delivery.AppId)
					requireEqual(t, "guest", delivery.UserId)
					requireEqual(t, now, delivery.Timestamp)
					requireEqual(t, "1234567890", delivery.MessageId)
					requireEqual(t, "0987654321", delivery.CorrelationId)
					requireEqual(t, "test-content-type", delivery.ContentType)
					requireEqual(t, "test-content-encoding", delivery.ContentEncoding)
					requireEqual(t, "test-type", delivery.Type)
					requireEqual(t, "20000", delivery.Expiration)
					requireEqual(t, "for-rpc-clients", delivery.ReplyTo)
					requireEqual(t, gorabbitmq.Table{"test-header": "test-value"}, gorabbitmq.Table(delivery.Headers))

					wg.Done()

					return gorabbitmq.Ack
				}
			},
			getPublisher: func(c *gorabbitmq.Connector) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionAppID("test-service"),
					gorabbitmq.WithPublishOptionUserID("guest"),
					gorabbitmq.WithPublishOptionTimestamp(now),
					gorabbitmq.WithPublishOptionMessageID("1234567890"),
					gorabbitmq.WithPublishOptionTracing("0987654321"),
					gorabbitmq.WithPublishOptionContentType("test-content-type"),
					gorabbitmq.WithPublishOptionContentEncoding("test-content-encoding"),
					gorabbitmq.WithPublishOptionType("test-type"),
					gorabbitmq.WithPublishOptionExpiration("20000"),
					gorabbitmq.WithPublishOptionReplyTo("for-rpc-clients"),
					gorabbitmq.WithPublishOptionHeaders(gorabbitmq.Table{
						"test-header": "test-value",
					}),
				)
			},
			publish: func(p *gorabbitmq.Publisher, targets []string) error {
				return p.PublishWithOptions(context.TODO(), targets, message)
			},
			targets: []string{stringGen(), stringGen()},
			message: message,
		},
		"publish to multiple with options": {
			connector: func() *gorabbitmq.Connector {
				amqpConfig := gorabbitmq.Config{
					Properties: amqp.Table{},
				}
				amqpConfig.Properties.SetClientConnectionName(stringGen())

				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithCustomConnectorOptions(
					&gorabbitmq.ConnectorOptions{
						ReturnHandler: nil,
						LogHandler: slog.NewTextHandler(
							os.Stdout,
							&slog.HandlerOptions{
								Level: logLevel,
							},
						),
						Config:            &amqpConfig,
						Codec:             nil,
						PrefetchCount:     0,
						ReconnectInterval: 0,
					},
				))
				requireNoError(t, err)

				return connector
			}(),
			deliveryHandler: func(expectedMessage any, wg *sync.WaitGroup) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)
					requireEqual(t, "messageID", d.MessageId)
					requireEqual(t, "correlationID", d.CorrelationId)
					requireEqual(t, now, d.Timestamp)

					wg.Done()

					return gorabbitmq.Ack
				}
			},
			getPublisher: func(c *gorabbitmq.Connector) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher()
			},
			publish: func(p *gorabbitmq.Publisher, targets []string) error {
				return p.PublishWithOptions(
					context.TODO(),
					targets,
					message,
					gorabbitmq.WithCustomPublishOptions(
						&gorabbitmq.PublishOptions{
							MessageID:     "messageID",
							CorrelationID: "correlationID",
							Timestamp:     now,
							AppID:         "service-name",
							UserID:        "guest",
							ContentType:   "text/plain",
							Mandatory:     false,
							Headers: gorabbitmq.Table{
								"test-header": "test-header-value",
							},
							Exchange:        gorabbitmq.ExchangeDefault,
							Expiration:      "200000",
							ContentEncoding: "",
							ReplyTo:         "for-rpc-servers",
							Type:            "",
							Priority:        gorabbitmq.NoPublishingPriority,
							DeliveryMode:    gorabbitmq.TransientDelivery,
						},
					),
				)
			},
			targets: []string{stringGen(), stringGen()},
			message: message,
		},
	}

	for name, test := range tests {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			t.Cleanup(func() {
				err := test.connector.Close()
				requireNoError(t, err)
			})

			wg := &sync.WaitGroup{}

			wg.Add(2)

			_, err := test.connector.NewConsumerAndSubscribe(
				test.targets[0],
				test.deliveryHandler(test.message, wg),
				gorabbitmq.WithQueueOptionAutoDelete(true),
				gorabbitmq.WithConsumerOptionConsumerName(fmt.Sprintf("my_consumer_%s", stringGen())),
			)
			requireNoError(t, err)

			_, err = test.connector.NewConsumerAndSubscribe(test.targets[1], test.deliveryHandler(test.message, wg), gorabbitmq.WithCustomConsumeOptions(
				&gorabbitmq.ConsumeOptions{
					ConsumerOptions: &gorabbitmq.ConsumerOptions{
						Args: make(gorabbitmq.Table),
						Name: stringGen(),
					},
					QueueOptions: &gorabbitmq.QueueOptions{
						Args:       make(gorabbitmq.Table),
						AutoDelete: true,
						Declare:    true,
					},
					ExchangeOptions: &gorabbitmq.ExchangeOptions{
						Args: make(gorabbitmq.Table),
						Name: gorabbitmq.ExchangeDefault,
						Kind: amqp.ExchangeDirect,
					},
					Bindings:        []gorabbitmq.Binding{},
					HandlerQuantity: 1,
				},
			))
			requireNoError(t, err)

			publisher, err := test.getPublisher(test.connector)
			requireNoError(t, err)

			err = test.publish(publisher, test.targets)
			requireNoError(t, err)

			wg.Wait()
		})
	}
}

func Test_Integration_ManualRemoveExchangeQueueAndBindings(t *testing.T) {
	t.Parallel()

	type testParams struct {
		exchangeName string
		queueName    string
		routingKey   string
	}

	tests := map[string]struct {
		testParams  *testParams
		connector   *gorabbitmq.Connector
		getConsumer func(*gorabbitmq.Connector, *testParams) (*gorabbitmq.Consumer, error)
		action      func(*gorabbitmq.Consumer, *testParams) error
	}{
		"remove queue": {
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			getConsumer: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(params.queueName, nil)
			},
			action: func(consumer *gorabbitmq.Consumer, params *testParams) error {
				removedMessages, err := consumer.RemoveQueue(params.queueName, false, false, false)
				requireNoError(t, err)

				requireEqual(t, 0, removedMessages)

				return nil
			},
			testParams: &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			},
		},
		"remove exchange": {
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			getConsumer: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					nil,
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionDurable(true),
					gorabbitmq.WithExchangeOptionKind(amqp.ExchangeDirect),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
				)
			},
			action: func(consumer *gorabbitmq.Consumer, params *testParams) error {
				err := consumer.RemoveExchange(params.exchangeName, false, false)
				requireNoError(t, err)

				return nil
			},
			testParams: &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			},
		},
		"remove binding": {
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
			getConsumer: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					nil,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(amqp.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithConsumerOptionBinding(gorabbitmq.Binding{
						RoutingKey: params.routingKey,
						BindingOptions: &gorabbitmq.BindingOptions{
							Args:    gorabbitmq.Table{},
							NoWait:  false,
							Declare: true,
						},
					}),
				)
			},
			action: func(consumer *gorabbitmq.Consumer, params *testParams) error {
				err := consumer.RemoveBinding(params.queueName, params.routingKey, params.exchangeName, nil)
				requireNoError(t, err)

				return nil
			},
			testParams: &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			},
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			t.Cleanup(func() {
				err := test.connector.Close()
				requireNoError(t, err)
			})

			consumer, err := test.getConsumer(test.connector, test.testParams)
			requireNoError(t, err)

			err = test.action(consumer, test.testParams)
			requireNoError(t, err)
		})
	}
}

func Test_Integration_SubscribingTwiceReturnsError(t *testing.T) {
	t.Parallel()

	connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
		UserName: "guest",
		Password: "guest",
		Host:     "localhost",
		Port:     5672,
	}, gorabbitmq.WithConnectorOptionLogHandler(
		slog.NewTextHandler(
			os.Stdout,
			&slog.HandlerOptions{
				Level: logLevel,
			},
		),
	))
	requireNoError(t, err)

	t.Cleanup(func() {
		err := connector.Close()
		requireNoError(t, err)
	})

	consumer, err := connector.NewConsumerAndSubscribe(
		"test-queue",
		nil,
		gorabbitmq.WithQueueOptionAutoDelete(true),
	)
	requireNoError(t, err)

	err = consumer.Subscribe(nil)

	requireEqual(t, gorabbitmq.ErrAlreadySubscribed, cause(err))

	err = consumer.Unsubscribe()
	requireNoError(t, err)
}

func Test_Integration_ReturnHandler(t *testing.T) {
	t.Parallel()

	message := "test-message"

	doneChan := make(chan struct{})

	returnHandler := func(r gorabbitmq.Return) {
		requireEqual(t, message, string(r.Body))
		requireEqual(t, "text/plain", r.ContentType)

		doneChan <- struct{}{}
	}

	connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
		UserName: "guest",
		Password: "guest",
		Host:     "localhost",
		Port:     5672,
	}, gorabbitmq.WithConnectorOptionLogHandler(
		slog.NewTextHandler(
			os.Stdout,
			&slog.HandlerOptions{
				Level: logLevel,
			},
		),
	),
		gorabbitmq.WithConnectorOptionReturnHandler(returnHandler),
	)
	requireNoError(t, err)

	t.Cleanup(func() {
		err := connector.Close()
		requireNoError(t, err)
	})

	exchangeName := stringGen()
	queueName := stringGen()
	routingKey := stringGen()

	_, err = connector.NewConsumerAndSubscribe(
		queueName,
		nil,
		gorabbitmq.WithExchangeOptionDeclare(true),
		gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
		gorabbitmq.WithExchangeOptionName(exchangeName),
		gorabbitmq.WithConsumerOptionRoutingKey(routingKey),
		gorabbitmq.WithQueueOptionAutoDelete(true),
		gorabbitmq.WithExchangeOptionAutoDelete(true),
	)
	requireNoError(t, err)

	publisher, err := connector.NewPublisher(
		gorabbitmq.WithPublishOptionExchange(exchangeName),
		gorabbitmq.WithPublishOptionMandatory(true),
	)
	requireNoError(t, err)

	err = publisher.Publish(context.TODO(), "does-not-exist", message)
	requireNoError(t, err)

	<-doneChan
}

func Test_DecodeDeliveryBody(t *testing.T) {
	t.Parallel()

	message := testData{
		Name:    "Name",
		Age:     157,
		City:    "City",
		Country: "Country",
	}

	jsonMessage, err := json.Marshal(&message)
	requireNoError(t, err)

	delivery := gorabbitmq.Delivery{
		Delivery: amqp.Delivery{
			ContentType: "application/json",
			Timestamp:   time.Now(),
			Body:        jsonMessage,
		},
	}

	tests := map[string]struct {
		connector *gorabbitmq.Connector
	}{
		"with standard codec": {
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				))
				requireNoError(t, err)

				return connector
			}(),
		},
		"with self-defined codec": {
			connector: func() *gorabbitmq.Connector {
				connector, err := gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
					UserName: "guest",
					Password: "guest",
					Host:     "localhost",
					Port:     5672,
				}, gorabbitmq.WithConnectorOptionLogHandler(
					slog.NewTextHandler(
						os.Stdout,
						&slog.HandlerOptions{
							Level: logLevel,
						},
					),
				),
					gorabbitmq.WithConnectorOptionEncoder(json.Marshal),
					gorabbitmq.WithConnectorOptionDecoder(json.Unmarshal),
				)
				requireNoError(t, err)

				return connector
			}(),
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var result testData

			err := test.connector.DecodeDeliveryBody(delivery, &result)
			requireNoError(t, err)

			requireEqual(t, message, result)
		})
	}
}

func requireEqual(t *testing.T, expected any, actual any) {
	t.Helper()

	equal := reflect.DeepEqual(expected, actual)

	if !equal {
		t.Errorf("Not equal: \nExpected: %v\nActual: %+v", expected, actual)
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Error(err)
	}
}

func cause(err error) error {
	type causer interface {
		Unwrap() error
	}

	for err != nil {
		cause, ok := err.(causer) //nolint: errorlint // expected behavior
		if !ok {
			break
		}

		err = cause.Unwrap()
	}

	return err
}

func stringGen() string {
	bytes := make([]byte, 16)

	_, err := rand.Read(bytes)
	if err != nil {
		return ""
	}

	return hex.EncodeToString(bytes)
}
