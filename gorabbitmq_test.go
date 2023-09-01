package gorabbitmq_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Clarilab/gorabbitmq/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

type testData struct {
	Name    string `json:"name"`
	Age     int    `json:"age"`
	City    string `json:"city"`
	Country string `json:"country"`
}

type testParams struct {
	exchangeName string
	queueName    string
	routingKey   string
}

func Test_Integration_PublishToExchange(t *testing.T) {
	t.Parallel()

	stringMessage := "test-message"
	bytesMessage := []byte(stringMessage)

	jsonMessage := testData{
		Name:    "Name",
		Age:     157,
		City:    "City",
		Country: "Country",
	}

	tests := map[string]struct {
		deliveryHandler func(any, chan struct{}) gorabbitmq.HandlerFunc
		getConsumer     func(*gorabbitmq.Connection, gorabbitmq.HandlerFunc, *testParams) (*gorabbitmq.Consumer, error)
		passiveExchange bool
		message         any
	}{
		"publish to exchange / consume with exchange NoWait": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
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

			message: stringMessage,
		},
		"publish to exchange passive": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, d.Body)
					requireEqual(t, "application/octet-stream", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
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
			passiveExchange: true,
			message:         bytesMessage,
		},
		"publish bytes message": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, d.Body)
					requireEqual(t, "application/octet-stream", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
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

			message: bytesMessage,
		},
		"publish json message": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, "application/json", d.ContentType)

					var result testData

					err := json.Unmarshal(d.Body, &result)
					requireNoError(t, err)

					requireEqual(t, expectedMessage, result)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
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
			message: jsonMessage,
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			publishConn := getConnection(t)
			consumerConn := getConnection(t)

			t.Cleanup(func() {
				err := publishConn.Close()
				requireNoError(t, err)

				err = consumerConn.Close()
				requireNoError(t, err)
			})

			doneChan := make(chan struct{})

			testParams := &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			}

			// connecting to a passive exchange requires the exchange to exist beforehand
			// so here the exchange gets declared before the binding is declared.

			if test.passiveExchange {
				consumer, err := gorabbitmq.NewConsumer(
					consumerConn,
					testParams.queueName,
					nil,
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(testParams.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(testParams.routingKey),
				)

				requireNoError(t, err)

				err = consumer.Close()
				requireNoError(t, err)
			}

			_, err := test.getConsumer(consumerConn, test.deliveryHandler(test.message, doneChan), testParams)
			requireNoError(t, err)

			publisher, err := gorabbitmq.NewPublisher(
				publishConn,
				gorabbitmq.WithPublishOptionExchange(testParams.exchangeName),
			)
			requireNoError(t, err)

			err = publisher.Publish(context.TODO(), testParams.routingKey, test.message)
			requireNoError(t, err)

			<-doneChan

			// cleaning up the passive exchange again
			if test.passiveExchange {
				err = consumerConn.RemoveExchange(testParams.exchangeName, false, false)
				requireNoError(t, err)
			}
		})
	}
}

func Test_Integration_PublishToQueue(t *testing.T) {
	t.Parallel()

	message := "test-message"

	tests := map[string]struct {
		deliveryHandler func(any, chan struct{}) gorabbitmq.HandlerFunc
		getConsumer     func(*gorabbitmq.Connection, gorabbitmq.HandlerFunc, string) (*gorabbitmq.Consumer, error)
		passiveQueue    bool
		publish         func(*gorabbitmq.Publisher, string) error
	}{
		"publish to queue": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionArgs(gorabbitmq.Table{
						"test-queue-arg-key": "test-queue-arg-value",
					}),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.PublishWithOptions(context.TODO(), []string{target}, message)
			},
		},
		"publish to queue passive": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionPassive(true),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.PublishWithOptions(context.TODO(), []string{target}, message)
			},
			passiveQueue: true,
		},
		"publish to queue NoWait": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionNoWait(true),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.PublishWithOptions(context.TODO(), []string{target}, message)
			},
		},
		"publish to priority queue": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)
					requireEqual(t, 4, int(d.Priority))

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionPriority(gorabbitmq.HighestPriority),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.PublishWithOptions(context.TODO(), []string{target}, message, gorabbitmq.WithPublishOptionPriority(gorabbitmq.HighPriority))
			},
		},
		"publish to durable queue": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionDurable(true),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.PublishWithOptions(context.TODO(), []string{target}, message)
			},
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			publishConn := getConnection(t)
			consumeConn := getConnection(t)

			t.Cleanup(func() {
				err := publishConn.Close()
				requireNoError(t, err)

				err = consumeConn.Close()
				requireNoError(t, err)
			})

			doneChan := make(chan struct{})
			queueName := stringGen()

			// connecting to a passive queue requires the queue to exist beforehand
			// so here the queue gets declared before the consumer subscribes.

			if test.passiveQueue {
				consumer, err := gorabbitmq.NewConsumer(consumeConn, queueName, nil)

				requireNoError(t, err)

				err = consumer.Close()
				requireNoError(t, err)
			}

			_, err := test.getConsumer(consumeConn, test.deliveryHandler(message, doneChan), queueName)
			requireNoError(t, err)

			publisher, err := gorabbitmq.NewPublisher(publishConn)
			requireNoError(t, err)

			err = test.publish(publisher, queueName)
			requireNoError(t, err)

			<-doneChan

			// cleaning up the passive queue again
			if test.passiveQueue {
				_, err = consumeConn.RemoveQueue(queueName, false, false, false)
				requireNoError(t, err)
			}
		})
	}
}

func Test_Integration_Consume(t *testing.T) {
	t.Parallel()

	message := "test-message"

	tests := map[string]struct {
		deliveryHandler func(any, int, chan struct{}) gorabbitmq.HandlerFunc
		getConsumer     func(*gorabbitmq.Connection, gorabbitmq.HandlerFunc, *testParams) (*gorabbitmq.Consumer, error)
	}{
		"consume with Ack": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
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
		},
		"consume with NackDisgard": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.NackDiscard
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
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
		},
		"consume with NackRequeue": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
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
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
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
		},
		"consume with Manual": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(delivery *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(delivery.Body))
					requireEqual(t, "text/plain", delivery.ContentType)

					doneChan <- struct{}{}

					err := delivery.Ack(false)
					requireNoError(t, err)

					return gorabbitmq.Manual
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
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
		},
		"consume with AutoAck": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Manual
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
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
		},
		"consume with consumer NoWait": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
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
		},
		"consume with multiple message handlers": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(conn *gorabbitmq.Connection, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
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
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			publishConn := getConnection(t)
			consumeConn := getConnection(t)

			t.Cleanup(func() {
				err := publishConn.Close()
				requireNoError(t, err)

				err = consumeConn.Close()
				requireNoError(t, err)
			})

			doneChan := make(chan struct{})

			testParams := &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			}

			var counter int

			_, err := test.getConsumer(consumeConn, test.deliveryHandler(message, counter, doneChan), testParams)
			requireNoError(t, err)

			publisher, err := gorabbitmq.NewPublisher(
				publishConn,
				gorabbitmq.WithPublishOptionExchange(testParams.exchangeName),
			)
			requireNoError(t, err)

			err = publisher.Publish(context.TODO(), testParams.routingKey, message)
			requireNoError(t, err)

			<-doneChan
		})
	}
}

func Test_Integration_CustomOptions(t *testing.T) {
	t.Parallel()

	message := "test-message"

	now := time.Date(2023, 8, 1, 12, 0, 0, 0, time.Local)

	tests := map[string]struct {
		publishConn     *gorabbitmq.Connection
		deliveryHandler func(any, *sync.WaitGroup) gorabbitmq.HandlerFunc
		getPublisher    func(*gorabbitmq.Connection) (*gorabbitmq.Publisher, error)
		publish         func(*gorabbitmq.Publisher, []string) error
	}{
		"publish with options": {
			publishConn: getConnection(t),
			deliveryHandler: func(expectedMessage any, wg *sync.WaitGroup) gorabbitmq.HandlerFunc {
				return func(delivery *gorabbitmq.Delivery) gorabbitmq.Action {
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
			getPublisher: func(conn *gorabbitmq.Connection) (*gorabbitmq.Publisher, error) {
				return gorabbitmq.NewPublisher(
					conn,
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
		},
		"publish with custom options": {
			publishConn: func() *gorabbitmq.Connection {
				amqpConfig := gorabbitmq.Config{
					Properties: amqp.Table{},
				}
				amqpConfig.Properties.SetClientConnectionName(stringGen())

				return getConnection(t, gorabbitmq.WithCustomConnectionOptions(
					&gorabbitmq.ConnectionOptions{
						ReturnHandler:     nil,
						Config:            &amqpConfig,
						PrefetchCount:     0,
						ReconnectInterval: 0,
					},
				))
			}(),
			deliveryHandler: func(expectedMessage any, wg *sync.WaitGroup) gorabbitmq.HandlerFunc {
				return func(d *gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)
					requireEqual(t, "messageID", d.MessageId)
					requireEqual(t, "correlationID", d.CorrelationId)
					requireEqual(t, now, d.Timestamp)

					wg.Done()

					return gorabbitmq.Ack
				}
			},
			getPublisher: func(conn *gorabbitmq.Connection) (*gorabbitmq.Publisher, error) {
				return gorabbitmq.NewPublisher(conn)
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
							Priority:        gorabbitmq.NoPriority,
							DeliveryMode:    gorabbitmq.TransientDelivery,
						},
					),
				)
			},
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			targets := []string{stringGen(), stringGen()}

			consumeConn := getConnection(t)

			t.Cleanup(func() {
				err := test.publishConn.Close()
				requireNoError(t, err)

				err = consumeConn.Close()
				requireNoError(t, err)
			})

			wg := &sync.WaitGroup{}

			// adding 2 to wait for both consumers to handle their deliveries.
			wg.Add(2)

			// registering first consumer.
			_, err := gorabbitmq.NewConsumer(
				consumeConn,
				targets[0],
				test.deliveryHandler(message, wg),
				gorabbitmq.WithQueueOptionAutoDelete(true),
				gorabbitmq.WithConsumerOptionConsumerName(fmt.Sprintf("my_consumer_%s", stringGen())),
			)
			requireNoError(t, err)

			// registering second consumer with custom options.
			_, err = gorabbitmq.NewConsumer(
				consumeConn,
				targets[1],
				test.deliveryHandler(message, wg),
				gorabbitmq.WithCustomConsumeOptions(
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
				),
			)
			requireNoError(t, err)

			publisher, err := test.getPublisher(test.publishConn)
			requireNoError(t, err)

			// publishing to multiple targets
			err = test.publish(publisher, targets)
			requireNoError(t, err)

			wg.Wait()
		})
	}
}

func Test_Integration_ManualRemoveExchangeQueueAndBindings(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		getConsumer func(*gorabbitmq.Connection, *testParams) (*gorabbitmq.Consumer, error)
		action      func(*gorabbitmq.Connection, *testParams) error
	}{
		"remove queue": {
			getConsumer: func(conn *gorabbitmq.Connection, params *testParams) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(conn, params.queueName, nil)
			},
			action: func(conn *gorabbitmq.Connection, params *testParams) error {
				removedMessages, err := conn.RemoveQueue(params.queueName, false, false, false)
				requireNoError(t, err)

				requireEqual(t, 0, removedMessages)

				return nil
			},
		},
		"remove exchange": {
			getConsumer: func(conn *gorabbitmq.Connection, params *testParams) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
					params.queueName,
					nil,
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionDurable(true),
					gorabbitmq.WithExchangeOptionKind(amqp.ExchangeDirect),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
				)
			},
			action: func(conn *gorabbitmq.Connection, params *testParams) error {
				err := conn.RemoveExchange(params.exchangeName, false, false)
				requireNoError(t, err)

				return nil
			},
		},
		"remove binding": {
			getConsumer: func(conn *gorabbitmq.Connection, params *testParams) (*gorabbitmq.Consumer, error) {
				return gorabbitmq.NewConsumer(
					conn,
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
			action: func(conn *gorabbitmq.Connection, params *testParams) error {
				err := conn.RemoveBinding(params.queueName, params.routingKey, params.exchangeName, nil)
				requireNoError(t, err)

				return nil
			},
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testParams := &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			}

			conn := getConnection(t)

			t.Cleanup(func() {
				err := conn.Close()
				requireNoError(t, err)
			})

			_, err := test.getConsumer(conn, testParams)
			requireNoError(t, err)

			err = test.action(conn, testParams)
			requireNoError(t, err)
		})
	}
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

	publishConn := getConnection(
		t,
		gorabbitmq.WithConnectionOptionReturnHandler(returnHandler),
		gorabbitmq.WithConnectionOptionTextLogging(os.Stdout, slog.LevelError),
		gorabbitmq.WithConnectionOptionConnectionName(stringGen()),
	)

	consumerConn := getConnection(t)

	t.Cleanup(func() {
		err := publishConn.Close()
		requireNoError(t, err)

		err = consumerConn.Close()
		requireNoError(t, err)
	})

	exchangeName := stringGen()
	queueName := stringGen()
	routingKey := stringGen()

	_, err := gorabbitmq.NewConsumer(
		consumerConn,
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

	publisher, err := gorabbitmq.NewPublisher(
		publishConn,
		gorabbitmq.WithPublishOptionExchange(exchangeName),
		gorabbitmq.WithPublishOptionMandatory(true),
	)
	requireNoError(t, err)

	// publishing a mandatory message with a routing key with out the existence of a binding.
	err = publisher.Publish(context.TODO(), "does-not-exist", message)
	requireNoError(t, err)

	// the publishing is retured to the return handler.

	// waiting for the return handler to process the message.
	<-doneChan
}

func Test_Integration_DecodeDeliveryBody(t *testing.T) {
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
		conn *gorabbitmq.Connection
	}{
		"with standard codec": {
			conn: getConnection(t),
		},
		"with self-defined codec": {
			conn: getConnection(
				t,
				gorabbitmq.WithConnectionOptionEncoder(json.Marshal),
				gorabbitmq.WithConnectionOptionDecoder(json.Unmarshal),
			),
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			t.Cleanup(func() {
				err := test.conn.Close()
				requireNoError(t, err)
			})

			var result testData

			err = test.conn.DecodeDeliveryBody(delivery, &result)
			requireNoError(t, err)

			requireEqual(t, message, result)
		})
	}
}

func Test_Integration_DeadLetterRetry(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		conn *gorabbitmq.Connection
	}{
		"with provided connection": {
			conn: getConnection(t),
		},
		"without provided connection": {
			conn: nil,
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testMessage := stringGen()
			exchangeName := stringGen()
			queueName := stringGen()
			routingKey := stringGen()

			publishConn := getConnection(t)
			consumeConn := getConnection(t)

			t.Cleanup(func() {
				err := publishConn.Close()
				requireNoError(t, err)

				err = consumeConn.Close()
				requireNoError(t, err)
			})

			publisher, err := gorabbitmq.NewPublisher(publishConn,
				gorabbitmq.WithPublishOptionExchange(exchangeName),
			)
			requireNoError(t, err)

			doneChan := make(chan struct{})

			handler := func(delivery *gorabbitmq.Delivery) gorabbitmq.Action {
				requireEqual(t, testMessage, string(delivery.Body))

				retryCount, _ := delivery.Headers["x-retry-count"].(int32)

				if retryCount < 2 {
					return gorabbitmq.NackDiscard
				}

				doneChan <- struct{}{}

				return gorabbitmq.Ack
			}

			consumer, err := gorabbitmq.NewConsumer(consumeConn, queueName, handler,
				gorabbitmq.WithExchangeOptionDeclare(true),
				gorabbitmq.WithExchangeOptionName(exchangeName),
				gorabbitmq.WithExchangeOptionName(exchangeName),
				gorabbitmq.WithExchangeOptionAutoDelete(true),
				gorabbitmq.WithConsumerOptionRoutingKey(routingKey),
				gorabbitmq.WithQueueOptionAutoDelete(true),
				gorabbitmq.WithConsumerOptionDeadLetterRetry(
					&gorabbitmq.RetryOptions{
						RetryConn: test.conn,
						Delays: []time.Duration{
							time.Second,
							time.Second * 2,
							time.Second * 3,
							time.Second * 4,
							time.Second * 5,
						},
						MaxRetries: 5,
						Cleanup:    true,
					},
				),
			)
			requireNoError(t, err)

			err = publisher.Publish(context.Background(), routingKey, testMessage)
			requireNoError(t, err)

			<-doneChan

			err = consumer.Close()
			requireNoError(t, err)
		})
	}
}

// testBuffer is used as buffer for the logging io.Writer
// with mutex protection for concurrent access.
type testBuffer struct {
	mtx  *sync.Mutex
	buff *bytes.Buffer
}

// Write implements io.Writer interface.
// Calls the underlying bytes.Buffer method with mutex protection.
func (tb *testBuffer) Write(p []byte) (int, error) {
	tb.mtx.Lock()
	defer tb.mtx.Unlock()

	return tb.buff.Write(p)
}

// ReadBytes calls the underlying bytes.Buffer method with mutex protection.
func (tb *testBuffer) ReadBytes(delim byte) ([]byte, error) {
	tb.mtx.Lock()
	defer tb.mtx.Unlock()

	return tb.buff.ReadBytes(delim)
}

// ReadBytes calls the underlying bytes.Buffer method with mutex protection.
func (tb *testBuffer) Reset() {
	tb.mtx.Lock()
	defer tb.mtx.Unlock()

	tb.buff.Reset()
}

// ReadBytes calls the underlying bytes.Buffer method with mutex protection.
func (tb *testBuffer) Len() int {
	tb.mtx.Lock()
	defer tb.mtx.Unlock()

	return tb.buff.Len()
}

// logEntry is the log entry that will be written to the buffer.
type logEntry struct {
	Time  time.Time `json:"time"`
	Level string    `json:"level"`
	Msg   string    `json:"msg"`
}

func Test_Reconnection_AutomaticReconnect(t *testing.T) { //nolint:paralleltest // intentional: must not run in parallel
	// used to wait until the handler processed the deliveries.
	doneChan := make(chan struct{})

	message := "test-message"

	// declaring a mutex protected publishConnLogBuffer.
	publishConnLogBuffer := &testBuffer{
		mtx:  &sync.Mutex{},
		buff: new(bytes.Buffer),
	}

	// declaring a mutex protected consumeConnLogBuffer.
	consumeConnLogBuffer := &testBuffer{
		mtx:  &sync.Mutex{},
		buff: new(bytes.Buffer),
	}

	// declaring the connections with JSON logging on debug level enabled.
	// (later used to compare if the reconnection was successful).
	publishConn := getConnection(t,
		gorabbitmq.WithConnectionOptionJSONLogging(publishConnLogBuffer, slog.LevelDebug),
	)

	consumeConn := getConnection(t,
		gorabbitmq.WithConnectionOptionJSONLogging(consumeConnLogBuffer, slog.LevelDebug),
	)

	t.Cleanup(func() {
		err := publishConn.Close()
		requireNoError(t, err)

		err = consumeConn.Close()
		requireNoError(t, err)
	})

	// msgCounter is used to count the number of deliveries, to compare it afterwords.
	var msgCounter int

	handler := func(msg *gorabbitmq.Delivery) gorabbitmq.Action {
		requireEqual(t, message, string(msg.Body))

		msgCounter++

		doneChan <- struct{}{}

		return gorabbitmq.Ack
	}

	queueName := stringGen()

	// creating a consumer.
	_, err := gorabbitmq.NewConsumer(consumeConn, queueName, handler,
		gorabbitmq.WithQueueOptionDurable(true),
		gorabbitmq.WithConsumerOptionConsumerName(stringGen()),
	)
	requireNoError(t, err)

	// creating a publisher.
	publisher, err := gorabbitmq.NewPublisher(publishConn)
	requireNoError(t, err)

	// publish a message.
	err = publisher.Publish(context.Background(), queueName, message)
	requireNoError(t, err)

	// waiting for the handler to process the delivery.
	<-doneChan

	// comparing the msgCounter that should be incremented by the consumer handler.
	requireEqual(t, 1, msgCounter)

	// shutting down the rabbitmq container to simulate a connection loss.
	err = exec.Command("docker", "compose", "down", "rabbitmq").Run()
	requireNoError(t, err)

	// bringing the rabbitmq container up again.
	err = exec.Command("docker", "compose", "up", "-d").Run()
	requireNoError(t, err)

	// While trying to reconnect, the logger writes information about the reconnection state on debug level.
	// In the following routines, the buffer given to the logger is read until the msg in the
	// log-entry states that reconnection was successful.

	wg := &sync.WaitGroup{}

	wg.Add(2)

	// reading the logs
	go watchConnLogBuffer(publishConnLogBuffer, wg)
	go watchConnLogBuffer(consumeConnLogBuffer, wg)

	// waiting for the both connections to be successfully recovered.
	wg.Wait()

	// publish a new message to the queue with the reconnected .
	err = publisher.Publish(context.Background(), queueName, message)
	requireNoError(t, err)

	// waiting for the recovered consumer to process the new message.
	<-doneChan

	// comparing the msgCounter again that should now be incremented by the consumer handler to 2.
	requireEqual(t, 2, msgCounter)
}

func watchConnLogBuffer(buffer *testBuffer, wg *sync.WaitGroup) {
	for {
		line, err := buffer.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			continue
		}

		var logEntry logEntry

		_ = json.Unmarshal(line, &logEntry)

		if buffer.Len() == 0 {
			buffer.Reset()
		}

		if logEntry.Msg == "successfully recovered connection" {
			wg.Done()

			break
		}
	}
}

func Test_Reconnection_AutomaticReconnectFailedTryManualReconnect(t *testing.T) { //nolint:paralleltest // intentional: must not run in parallel
	// used to wait until the handler processed the deliveries.
	doneChan := make(chan struct{})

	message := "test-message"

	// declaring the connections with a maximum of 1 reconnection attempts.
	publishConn := getConnection(t,
		gorabbitmq.WithConnectionOptionMaxReconnectRetries(1),
	)

	consumeConn := getConnection(t,
		gorabbitmq.WithConnectionOptionMaxReconnectRetries(1),
	)

	t.Cleanup(func() {
		err := publishConn.Close()
		requireNoError(t, err)

		err = consumeConn.Close()
		requireNoError(t, err)
	})

	// msgCounter is used to count the number of deliveries, to compare it afterwords.
	var msgCounter int

	handler := func(msg *gorabbitmq.Delivery) gorabbitmq.Action {
		requireEqual(t, message, string(msg.Body))

		msgCounter++

		doneChan <- struct{}{}

		return gorabbitmq.Ack
	}

	queueName := stringGen()

	// creating a consumer.
	_, err := gorabbitmq.NewConsumer(consumeConn, queueName, handler,
		gorabbitmq.WithQueueOptionDurable(true),
		gorabbitmq.WithConsumerOptionConsumerName(stringGen()),
	)
	requireNoError(t, err)

	// creating a publisher.
	publisher, err := gorabbitmq.NewPublisher(publishConn)
	requireNoError(t, err)

	// publish a message.
	err = publisher.Publish(context.Background(), queueName, message)
	requireNoError(t, err)

	publishNotifyChan := publishConn.NotifyAutoRecoveryFail()
	consumeNotifyChan := consumeConn.NotifyAutoRecoveryFail()

	wg := &sync.WaitGroup{}

	wg.Add(2)

	// handling the failed recovery notification.
	go handleFailedRecovery(publishNotifyChan, wg)
	go handleFailedRecovery(consumeNotifyChan, wg)

	// waiting for the handler to process the delivery.
	<-doneChan

	// comparing the msgCounter that should be incremented by the consumer handler.
	requireEqual(t, 1, msgCounter)

	// shutting down the rabbitmq container to simulate a connection loss.
	err = exec.Command("docker", "compose", "down", "rabbitmq").Run()
	requireNoError(t, err)

	// waiting for the failed recovery notification to finish handling.
	wg.Wait()

	// bringing the rabbitmq container up again.
	err = exec.Command("docker", "compose", "up", "-d").Run()
	requireNoError(t, err)

	// polling to check the container health.
	for range time.NewTicker(1 * time.Second).C {
		status, err := exec.Command("docker", "inspect", "-f", "{{.State.Health.Status}}", "rabbitmq").Output()
		requireNoError(t, err)

		if strings.ReplaceAll(string(status), "\n", "") == "healthy" {
			break
		}
	}

	// manually reconnecting.
	err = publishConn.Reconnect()
	requireNoError(t, err)

	err = consumeConn.Reconnect()
	requireNoError(t, err)

	// publish a new message to the queue with the reconnected .
	err = publisher.Publish(context.Background(), queueName, message)
	requireNoError(t, err)

	// waiting for the recovered consumer to process the new message.
	<-doneChan

	// comparing the msgCounter again that should now be incremented by the consumer handler to 2.
	requireEqual(t, 2, msgCounter)
}

func handleFailedRecovery(chn <-chan error, wg *sync.WaitGroup) {
	for range chn {
		wg.Done()
	}
}

// ##### helper functions: ##########################

// Returns a new connection with the given options.
func getConnection(t *testing.T, options ...gorabbitmq.ConnectionOption) *gorabbitmq.Connection {
	t.Helper()

	conn, err := gorabbitmq.NewConnection(gorabbitmq.SettingsToURI(&gorabbitmq.ConnectionSettings{
		UserName: "guest",
		Password: "guest",
		Host:     "localhost",
		Port:     5672,
	}),
		options...,
	)

	requireNoError(t, err)

	return conn
}

// Compares two values and reports an error if they are not equal.
func requireEqual(t *testing.T, expected any, actual any) {
	t.Helper()

	equal := reflect.DeepEqual(expected, actual)

	if !equal {
		t.Errorf("Not equal: \nExpected: %v\nActual: %+v", expected, actual)
	}
}

// Ensures that err is nil, otherwise it reports an error.
func requireNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Error(err)
	}
}

// Generates a random string for names (e.g. queue-names, exchange-names, routing-keys)
// that need to be unique since almost all tests run in parallel.
func stringGen() string {
	buffer := make([]byte, 16)

	_, err := rand.Read(buffer)
	if err != nil {
		return ""
	}

	return hex.EncodeToString(buffer)
}
