package gorabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	TransientDelivery DeliveryMode = iota + 1
	PersistentDelivery
)

const (
	// Ack default ack this msg after you have successfully processed this delivery.
	Ack Action = iota
	// NackDiscard the message will be dropped or delivered to a server configured dead-letter queue.
	NackDiscard
	// NackRequeue deliver this message to a different consumer.
	NackRequeue
	// Message acknowledgement is left to the user using the msg.Ack() method.
	Manual
)

const (
	NoPublishingPriority Priority = iota
	LowestPublishingPriority
	LowPublishingPriority
	MediumPublishingPriority
	HighPublishingPriority
	HighestPublishingPriority
)

const (
	ExchangeDefault string = amqp.DefaultExchange
	ExchangeDirect  string = amqp.ExchangeDirect
	ExchangeFanout  string = amqp.ExchangeFanout
	ExchangeTopic   string = amqp.ExchangeTopic
	ExchangeHeaders string = amqp.ExchangeHeaders
)

type (
	// The delivery mode of a message can be either transient ord persistent.
	DeliveryMode uint8

	// Priority of a message can be either no priority, lowest, low, medium, high or highest.
	Priority uint8

	// Action is an action that occurs after processed this delivery.
	Action int

	// Return captures a flattened struct of fields returned by the server when a Publishing is unable
	// to be delivered due to the `mandatory` flag set and no route found.
	Return amqp.Return

	// Table stores user supplied fields of the following types:
	//
	//	bool
	//	byte
	//	float32
	//	float64
	//	int
	//	int16
	//	int32
	//	int64
	//	nil
	//	string
	//	time.Time
	//	amqp.Decimal
	//	amqp.Table
	//	[]byte
	//	[]interface{} - containing above types
	//
	// Functions taking a table will immediately fail when the table contains a
	// value of an unsupported type.
	//
	// The caller must be specific in which precision of integer it wishes to
	// encode.
	//
	// Use a type assertion when reading values from a table for type conversion.
	//
	// RabbitMQ expects int32 for integer values.
	Table map[string]any
)
