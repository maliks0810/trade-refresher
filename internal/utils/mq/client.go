package mq

import (
	"context"
	"errors"
	"fmt"
)

type publishAPI struct {
	connectionStr 	string
	connection 		*connection
	publisher 		*publisher
}

type consumeAPI struct {
	connectionStr 	string
	connection 		*connection
	consumer 		*consumer
}

// Publisher provides communication methods to publish messages to an AMQP service
type Publisher interface {
	// Open is the recommended default configuration to open an AMQP connection and create a publisher channel with a named exchange with the appropriate exchange type/kind.
	//
	// Parameters:
	//
	// ExchangeName - the unique identifier of the exchange
	Open(string) error
	// OpenWithOptions will open an AMQP connection and create a publisher channel with the provided configurations.  This method is only recommended if you require
	// more specific configurations outside of the default Open() and assumes intimate knowledge of managing AMQP exchanges, queues, and bindings
	//
	// Parameters:
	//
	// func(*PublisherOptions) - an optional list of PublisherOption configuration values
	OpenWithOptions(...func(*PublisherOptions)) error

	// Close will close the publisher channel and and close the AMQP connection.
	Close() error
	
	// Publish will submit a message to the exchange configured during the Open/OpenWithOptions via the publisher channel
	//
	// Parameters:
	//
	// context.Context - The interface for configuring deadlines, cancellations, and request-scoped validations
	//
	// data - The message contents (limited to 16MiB)
	//
	// queues - The collection of queue targets
	//
	// opts - The optional list of PublishOptions to configure the publish action
	Publish(context.Context, []byte, []string, ...func(*PublishOptions)) error
}

// Consumer provides communication methods to consume messages from an AMQP service
type Consumer interface {
	// Open is the recommended default configuration to open an AMQP connection and create a consumer channel with a named exchange with the appropriate exchange type/kind
	// and the named queue to consume messages
	//
	// Parameters:
	//
	// ExchangeName - the unique identifier of the exchange
	//
	// QueueName - the unique identifier of the queue
	Open(string, string) error
	// OpenWithOptions will open an AMQP connection and create a consumer channel with the provided configurations.  This method is only recommended if you require
	// more specific configurations outside of the default Open() and assumes intimate knowledge of managing AMQP exchanges, queues, and bindings
	//
	// Parameters:
	//
	// func(*ConsumerOptions) - an optional list of ConsumerOptions configuration values
	OpenWithOptions(...func(*ConsumerOptions)) error

	// Close will close the consumer channel and and close the AMQP connection.
	Close() error

	// Run will continuously pull messages from the configured queue.  Once processing of the message is complete, the message should be acknowledged so that the 
	// message is completely removed from the queue.  IMPORTANT: This is a blocking method, and thus should be run within a separate goroutine.
	//
	// Parameters:
	//
	// func(Delivery) Action - Handler for processing the message and returning an acknowledgement on success
	Run(func(Delivery) Action) error
}

// NewPublisher will construct a Publisher interface with the provided AMQP connection string (provided by ES-PlatformEngineering)
func NewPublisher(connstr string) (Publisher, error) {
	if connstr == "" {
		return nil, errors.New("invalid AMQP connection string provided")
	}
	return &publishAPI{
		connectionStr: connstr,
		connection: nil,
		publisher: nil,
	}, nil
}

// Open implements Publisher Open method
func (p *publishAPI) Open(exchangeName string) error {
	if exchangeName == "" {
		return errors.New("invalid exchange name - name cannot be empty")
	}

	return p.OpenWithOptions(
		WithPublisherOptionsExchangeKind(Direct.String()),
		WithPublisherOptionsExchangeName(exchangeName),
		WithPublisherOptionsExchangeDeclare,
		WithPublisherOptionsExchangeAutoDelete,
	)
}

// OpenWithOptions implements Publisher OpenWithOptions method
func (p *publishAPI) OpenWithOptions(opts ...func(*PublisherOptions)) error {
	if p.connection != nil {
		return errors.New("unable to open a connection - a connection is already open with this client")
	}
	conn, err := newConnection(p.connectionStr)
	if err != nil {
		return err
	}

	pb, err := newPublisher(conn, opts...)
	if err != nil {
		return fmt.Errorf("unable to create a new publisher: %w", err)
	}

	p.connection = conn
	p.publisher = pb

	return nil
}

// Close implements Publisher Close method
func (p *publishAPI) Close() error {
	if p.publisher != nil {
		p.publisher.Close()
	}
	if p.connection != nil {
		p.connection.Close()
	}
	return errors.New("connection not found - unable to close")
}

// Publish implements Publisher Publish method
func (p *publishAPI) Publish(context context.Context, data []byte, routingKeys []string, opts ...func(*PublishOptions)) error {
	if p.connection != nil && p.publisher != nil {
		return p.publisher.Publish(context, data, routingKeys, opts...)
	}

	return errors.New("unable to publish a message - connection/publisher not found")
}

func NewConsumer(connstr string) (Consumer, error) {
	if connstr == "" {
		return nil, errors.New("invalid AMQP connection string provided")
	}
	return &consumeAPI{
		connectionStr: connstr,
		connection: nil,
		consumer: nil,
	}, nil
}

// Open implements Consumer Open method
func (c *consumeAPI) Open(exchangeName string, queueName string) error {
	if exchangeName == "" {
		return errors.New("invalid exchange name - cannot be empty")
	}
	if queueName == "" {
		return errors.New("invalid queue name - cannot be empty")
	}
	return c.OpenWithOptions(
		WithConsumerOptionsExchangeKind(Direct.String()),
		WithConsumerOptionsExchangeName(exchangeName),
		WithConsumerOptionsExchangeDeclare,
		WithConsumerOptionsExchangeAutoDelete,
		WithConsumerOptionsQueueOptions(
			WithQueueOptionsName(queueName),
			WithQueueOptionsDeclare(true),
			WithQueueOptionsAutoDelete(true),
		),
		WithConsumerOptionsBinding(
			Binding{ RoutingKey: queueName, Options: BindingOptions{ Declare: true } },
		),
	)
}

// OpenWithOptions implements Consumer OpenWithOptions method
func (c *consumeAPI) OpenWithOptions(opts ...func(*ConsumerOptions)) error {
	if c.connection != nil {
		return errors.New("unable to open a connection - a connection is already open with this client")
	}
	conn, err := newConnection(c.connectionStr)
	if err != nil {
		return err
	}

	cm, err := newConsumer(conn, opts...)
	if err != nil {
		return fmt.Errorf("unable to create a new consumer: %w", err)
	}

	c.connection = conn
	c.consumer = cm

	return nil
}

// Close implements Consumer Close methods
func (c *consumeAPI) Close() error {
	if c.consumer != nil {
		c.consumer.Close()
	}
	if c.connection != nil {
		return c.Close()
	}

	return errors.New("connection not found - unable to close")
}

// Run implements Consumer Run method
func (c *consumeAPI) Run(delivery func(Delivery) Action) error {
	if c.connection != nil {
		return c.consumer.Run(delivery)
	}

	return errors.New("unable to run consumer - AMQP connection not found")
}
