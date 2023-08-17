package gorabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// TransientDelivery indicates that the messsage should be published as transient message.
	TransientDelivery DeliveryMode = iota + 1
	// PersistentDelivery indicates that the messsage should be published as persistent message.
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
	// NoPriority indicates that the message should be published with no priority.
	NoPriority Priority = iota
	// LowestPriority indicates that the message should be published with lowest priority.
	LowestPriority
	// LowPriority indicates that the message should be published with low priority.
	LowPriority
	// NormalPriority indicates that the message should be published with normal priority.
	MediumPriority
	// HighPriority indicates that the message should be published with high priority.
	HighPriority
	// HighestPriority indicates that the message should be published with highest priority.
	HighestPriority
)

const (
	// Constant for RabbitMQ's default exchange (direct exchange).
	ExchangeDefault string = amqp.DefaultExchange
	// Constant for standard AMQP 0-9-1 direct exchange type.
	ExchangeDirect string = amqp.ExchangeDirect
	// Constant for standard AMQP 0-9-1 fanout exchange type.
	ExchangeFanout string = amqp.ExchangeFanout
	// Constant for standard AMQP 0-9-1 topic exchange type.
	ExchangeTopic string = amqp.ExchangeTopic
	// Constant for standard AMQP 0-9-1 headers exchange type.
	ExchangeHeaders string = amqp.ExchangeHeaders
)

type (
	// The delivery mode of a message can be either transient or persistent.
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
