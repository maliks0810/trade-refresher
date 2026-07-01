package mq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"refresher/trade-refresher/internal/utils/log"
) 

const (
	Ack Action = iota
	NackDiscard
	NackRequeue
	Manual
)

type consumer struct {
	chanManager 			*channelManager
	reconnectErrCh 			<-chan error
	closeConnectionCh 		chan<- struct{}
	options 				ConsumerOptions
	handleMu 				*sync.RWMutex
	isClosedMu 				*sync.RWMutex
	isClosed				bool		
}

// ConsumerOptions provides custom configuration options for the Consumer channel
type ConsumerOptions struct {
	AmqpOptions				AmqpOptions			// Specific AMQP configuration options
	QueueOptions 			QueueOptions		// Specific AMQP Queue configuration options
	CloseGracefully			bool				// When true, will wait for any outstanding consumer handlers to finish before closing the channel
	ExchangeOptions			[]ExchangeOptions	// Specific AMQP Exchange configuration options
	Concurrency 			int					// Number of goroutines to automatically configure for message handlers
	logger 					*zap.Logger			// Internal logger utility tool
	QosPrefetch 			int					// Number of messages to deliver before acknowledgments are received
	QosGlobal				bool				// Number of bytes of deliveries to flush to the network before acknowledgements are received
}

// AmqpOptions provides custom configuration options for the AMQP channel
type AmqpOptions struct {
	Name 					string				// Name of the consumer channel
	AutoAck 				bool				// When true, will automatically acknowledge a message (do not send an Ack)
	Exclusive 				bool				// When true, the AMQP server will ensure this is the sole consumer for the queue
	NoWait 					bool				// When true, the channel will not wait for the server to confirm the request
	NoLocal					bool				// Not supported
	Args 					Table				// Additional configuration arguments for the AMQP consumer channel
}

type Action int
type Delivery amqp.Delivery
type Handler func(d Delivery) (Action)

func newConsumer(connection *connection, opts ...func(*ConsumerOptions)) (*consumer, error) {
	if connection.connManager == nil {
		return nil, errors.New("connection manager cannot be nil")
	}

	defaultOptions := getDefaultConsumerOptions()
	options := &defaultOptions
	for _, opt := range opts {
		opt(options)
	}

	chanManager, err := newChannelManager(connection.connManager, connection.connManager.ReconnectInterval)
	if err != nil {
		return nil, fmt.Errorf("unable to create a new channel: %w", err)
	}
	reconnectErrCh, closeCh := chanManager.NotifyReconnect()

	return &consumer{
		chanManager: 		chanManager,
		reconnectErrCh: 	reconnectErrCh,
		closeConnectionCh: 	closeCh,
		options: 			*options,
		handleMu: 			&sync.RWMutex{},	
		isClosedMu: 		&sync.RWMutex{},
		isClosed: 			false,
	}, nil
}

func (c *consumer) Run(handler Handler) error {
	h := func(d Delivery) (Action) {
		if !c.handleMu.TryRLock() {
			return NackRequeue
		}
		defer c.handleMu.RUnlock()
		return handler(d)
	}

	err := c.start(h, c.options)
	if err != nil {
		return err
	}

	for err := range c.reconnectErrCh {
		c.options.logger.Info(fmt.Sprintf("successful consumer recovery from: %v", err))
		err = c.start(h, c.options)
		if err != nil {
			return fmt.Errorf("unable to restart the consumer goroutine after cancel/close: %w", err)
		}
	}

	return nil
}

func (c *consumer) Close() {
	c.CloseWithContext(context.Background())
}

func (c *consumer) CloseWithContext(context context.Context) {
	if c.options.CloseGracefully {
		c.options.logger.Info("waiting for handler to finish")
		err := c.wait(context)
		if err != nil {
			c.options.logger.Warn(fmt.Sprintf("error while waiting for handler for finish: %v", err))
		}
	}
	c.cleanup()
}

func (c *consumer) start(handler Handler, options ConsumerOptions) error {
	c.isClosedMu.Lock()
	defer c.isClosedMu.Unlock()

	err := c.chanManager.Qos(options.QosPrefetch, 0, options.QosGlobal)
	if err != nil {
		return fmt.Errorf("unable to declare Qos: %w", err)
	}

	for _, exchangeOption := range options.ExchangeOptions {
		err := declareExchange(c.chanManager, exchangeOption)
		if err != nil {
			return fmt.Errorf("unable to declare exchange: %w", err)
		}
	}

	err = declareQueue(c.chanManager, options.QueueOptions)
	if err != nil {
		return fmt.Errorf("unable to declare queue: %w", err)
	}

	for _, exchangeOption := range options.ExchangeOptions {
		err = declareBindings(c.chanManager, exchangeOption.Name, exchangeOption.QueueOptions.Name, exchangeOption.Bindings)
		if err != nil {
			return fmt.Errorf("unable to declare bindings: %w", err)
		}
	}

	msgs, err := c.chanManager.Consume(
		options.QueueOptions.Name,
		options.AmqpOptions.Name,
		options.AmqpOptions.AutoAck,
		options.AmqpOptions.Exclusive,
		false,
		options.AmqpOptions.NoWait,
		toAMQPTable(options.AmqpOptions.Args),
	)
	if err != nil {
		return err
	}

	for i := 0; i < options.Concurrency; i++ {
		go handlerGoroutine(c, msgs, options, handler)
	}
	c.options.logger.Info(fmt.Sprintf("processing messages on %v goroutines", options.Concurrency))
	
	return nil
}

func (c *consumer) wait(context context.Context) error {
	if context.Err() != nil {
		return context.Err()
	}

	ch := make(chan struct{})
	go func() {
		c.handleMu.Lock()
		defer c.handleMu.Unlock()
		close(ch)
	}()
	select {
	case <-context.Done():
		return context.Err()
	case <-ch:
		return nil
	}
}

func (c *consumer) cleanup() {
	c.isClosedMu.Lock()
	defer c.isClosedMu.Unlock()
	
	c.isClosed = true
	err := c.chanManager.Close()
	if err != nil {
		c.options.logger.Warn(fmt.Sprintf("error while closing the channel: %v", err))
	}

	c.options.logger.Info("closing the consumer")
	go func() {
		c.closeConnectionCh <- struct{}{}
	}()
}

func (c *consumer) getIsClosed() bool {
	c.isClosedMu.RLock()
	defer c.isClosedMu.RUnlock()
	return c.isClosed
}

func WithConsumerOptionsQueueDurable(options *ConsumerOptions) {
	options.QueueOptions.Durable = true
}

func WithConsumerOptionsQueueAutoDelete(options *ConsumerOptions) {
	options.QueueOptions.AutoDelete = true
}

func WithConsumerOptionsQueueExclusive(options *ConsumerOptions) {
	options.QueueOptions.Exclusive = true
}

func WithConsumerOptionsQueueNoWait(options *ConsumerOptions) {
	options.QueueOptions.NoWait = true
}

func WithConsumerOptionsQueuePassive(options *ConsumerOptions) {
	options.QueueOptions.Passive = true
}

func WithConsumerOptionsQueueNoDeclare(options *ConsumerOptions) {
	options.QueueOptions.Declare = false
}

func WithConsumerOptionsQueueArgs(args Table) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.QueueOptions.Args = args
	}
}

func ensureExchangeOptions(options *ConsumerOptions) {
	if len(options.ExchangeOptions) == 0 {
		options.ExchangeOptions = append(options.ExchangeOptions, getDefaultExchangeOptions())
	}
}

func WithConsumerOptionsExchangeName(name string) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		options.ExchangeOptions[0].Name = name
	}
}

func WithConsumerOptionsExchangeKind(kind string) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		options.ExchangeOptions[0].Kind = kind
	}
}

func WithConsumerOptionsExchangeDurable(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].Durable = true
}

func WithConsumerOptionsExchangeAutoDelete(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].AutoDelete = true
}

func WithConsumerOptionsExchangeInternal(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].Internal = true
}

func WithConsumerOptionsExchangeNoWait(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].NoWait = true
}

func WithConsumerOptionsExchangeDeclare(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].Declare = true
}

func WithConsumerOptionsExchangePassive(options *ConsumerOptions) {
	ensureExchangeOptions(options)
	options.ExchangeOptions[0].Passive = true
}

func WithConsumerOptionsExchangeArgs(args Table) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		options.ExchangeOptions[0].Args = args
	}
}

func WithConsumerOptionsRoutingKey(routingKey string) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		options.ExchangeOptions[0].Bindings = append(options.ExchangeOptions[0].Bindings, Binding{
			RoutingKey:     routingKey,
			Options: getDefaultBindingOptions(),
		})
	}
}

func WithConsumerOptionsBinding(binding Binding) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		ensureExchangeOptions(options)
		options.ExchangeOptions[0].Bindings = append(options.ExchangeOptions[0].Bindings, binding)
	}
}

func WithConsumerOptionsExchangeOptions(exchangeOptions ExchangeOptions) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.ExchangeOptions = append(options.ExchangeOptions, exchangeOptions)
	}
}

func WithConsumerOptionsConcurrency(concurrency int) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.Concurrency = concurrency
	}
}

func WithConsumerOptionsConsumerName(consumerName string) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.AmqpOptions.Name = consumerName
	}
}

func WithConsumerOptionsConsumerAutoAck(autoAck bool) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.AmqpOptions.AutoAck = autoAck
	}
}

func WithConsumerOptionsConsumerExclusive(options *ConsumerOptions) {
	options.AmqpOptions.Exclusive = true
}

func WithConsumerOptionsConsumerNoWait(options *ConsumerOptions) {
	options.AmqpOptions.NoWait = true
}

func WithConsumerOptionsQosPrefetch(prefetchCount int) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		options.QosPrefetch = prefetchCount
	}
}

func WithConsumerOptionsQosGlobal(options *ConsumerOptions) {
	options.QosGlobal = true
}

func WithConsumerOptionsForceShutdown(options *ConsumerOptions) {
	options.CloseGracefully = false
}

func WithConsumerOptionsQueueQuorum(options *ConsumerOptions) {
	if options.QueueOptions.Args == nil {
		options.QueueOptions.Args = Table{}
	}

	options.QueueOptions.Args["x-queue-type"] = "quorum"
}

func WithConsumerOptionsQueueMessageExpiration(ttl time.Duration) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		if options.QueueOptions.Args == nil {
			options.QueueOptions.Args = Table{}
		}
		options.QueueOptions.Args["x-message-ttl"] = ttl.Milliseconds()
	}
}

func WithConsumerOptionsQueueOptions(opts ...func(*QueueOptions)) func(*ConsumerOptions) {
	return func(options *ConsumerOptions) {
		queueOptions := &QueueOptions{}
		for _, opt := range opts {
			opt(queueOptions)
		}
		options.QueueOptions = *queueOptions
	}
}

func handlerGoroutine(consumer *consumer, msgs <-chan amqp.Delivery, options ConsumerOptions, handler Handler) {
	for msg := range msgs {
		if consumer.getIsClosed() {
			break
		}

		if options.AmqpOptions.AutoAck {
			handler(Delivery(msg))
			continue
		}

		switch handler(Delivery(msg)) {
		case Ack:
			err := msg.Ack(false)
			if err != nil {
				consumer.options.logger.Error(fmt.Sprintf("cannot ack message: %v", err))
			}
		case NackDiscard:
			err := msg.Nack(false, false)
			if err != nil {
				consumer.options.logger.Error(fmt.Sprintf("cannot nack message: %v", err))
			}
		case NackRequeue:
			err := msg.Nack(false, true)
			if err != nil {
				consumer.options.logger.Error(fmt.Sprintf("cannot nack/requeue message: %v", err))
			}
		}
	}
	
	consumer.options.logger.Info("amqp goroutine closed")
}

func getDefaultConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		AmqpOptions: AmqpOptions{
			Name:      "",
			AutoAck:   false,
			Exclusive: false,
			NoWait:    false,
			NoLocal:   false,
			Args:      Table{},
		},
		QueueOptions: QueueOptions{
			Name:       "",
			Durable:    false,
			AutoDelete: false,
			Exclusive:  false,
			NoWait:     false,
			Passive:    false,
			Args:       Table{},
			Declare:    true,
		},
		ExchangeOptions: []ExchangeOptions{},
		Concurrency:     1,
		CloseGracefully: true,
		logger:          log.Logger,
		QosPrefetch:     10,
		QosGlobal:       false,
	}
}