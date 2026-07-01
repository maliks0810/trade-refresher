package mq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeType struct {
	slug 			string
}

var (
	Unknown 		= ExchangeType{""}
	FanOut 			= ExchangeType{"fanout"}
	Direct 			= ExchangeType{"direct"}
	Topic 			= ExchangeType{"topic"}
)

// ExchangeOptions provides custom configuration options for the AMQP exchange
type ExchangeOptions struct {
	Name 			string				// The unique name of the exchange
	Kind 			string				// The kind of the exchange (FanOut, Direct, Topic)
	Durable 		bool				// When true, will persist through server restarts
	AutoDelete 		bool				// When true, will automatically delete after a period of time when all connections are closed
	Internal 		bool				// When true, will not accept message publishes
	NoWait 			bool				// When true, the exchange will be assumed to be previously declared
	Passive 		bool				// When true, the exchange is assumed to already exist
	Args 			Table				// Additional AMQP configuration options
	Declare 		bool				// When true, will automatically declare the exchange
	QueueOptions 	QueueOptions		// Additional AMQP queue configuration options
	Bindings		[]Binding			// Route/Topic bindings
}

func (e ExchangeType) String() string {
	return e.slug
}

func FromString(s string) (ExchangeType, error) {
	switch s {
	case FanOut.slug:
		return FanOut, nil
	case Direct.slug:
		return Direct, nil
	case Topic.slug:
		return Topic, nil
	}

	return Unknown, fmt.Errorf("invalid exchange type: %s", s)
}

func getDefaultExchangeOptions() ExchangeOptions {
	return ExchangeOptions{
		Name:       	"",
		Kind:       	amqp.ExchangeDirect,
		Durable:    	false,
		AutoDelete: 	false,
		Internal:   	false,
		NoWait:     	false,
		Passive:    	false,
		Args:       	Table{},
		Declare:    	false,
		QueueOptions: 	QueueOptions{},
		Bindings:   	[]Binding{},
	}
}