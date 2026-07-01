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
	Transient  uint8 = amqp.Transient
	Persistent uint8 = amqp.Persistent
	MaxMsgSize uint  = 1048576
)

type ReturnResponse struct {
	amqp.Return
}

type Confirmation struct {
	amqp.Confirmation
	ReconnectionCount int
}

type PublisherConfirmation []*amqp.DeferredConfirmation

type PublishOptions struct {
	Exchange 				string				// The unique exchange name
	Mandatory 				bool				// When true, will fail on delivery if the queue binding is not found
	Immediate 				bool				// When true, will fail delivery if there is no consumer available to receive the message
	ContentType 			string				// MIME content type
	DeliveryMode 			uint8				// Transient (0 or 1) or Persistent (2)
	Expiration 				string				// TTL in milliseconds before the message is deleted if not consumed
	ContentEncoding 		string				// MIME content encoding
	Priority 				uint8				// 0 to 9
	CorrelationID 			string				// The unique correlation identifier
	ReplyTo 				string				// address to to reply to (ex: RPC)
	MessageID 				string				// The unique message identifier
	Timestamp 				time.Time			// message timestamp
	Type 					string				// message type name
	UserID 					string				// creating user id - ex: "guest"
	AppID 					string				// creating application id
	Headers 				Table
}

type PublisherOptions struct {
	ExchangeOptions 		ExchangeOptions		// Additional AMQP exchange configuration options
	logger 					*zap.Logger			// The internal logger utility
	ConfirmMode				bool				// When true, will notify a confirmation handler of publish confirmations
}

type publisher struct {
	chanManager 			*channelManager
	connManager 			*connectionManager
	reconnectErrCh 			<-chan error
	closeConnectionCh 		chan<- struct{}
	handlerMu 				*sync.RWMutex
	notifyReturnHandler		func(r ReturnResponse)
	notifyPublishHandler	func(c Confirmation)
	options 				PublisherOptions
}

func newPublisher(connection *connection, opts ...func(*PublisherOptions)) (*publisher, error) {
	if connection.connManager == nil {
		return nil, errors.New("connection manager cannot be nil")
	}

	defaultOptions := getDefaultPublisherOptions()
	options := &defaultOptions
	for _, opt := range opts {
		opt(options)
	}

	chanManager, err := newChannelManager(connection.connManager, connection.connManager.ReconnectInterval)
	if err != nil {
		return nil, fmt.Errorf("unable to create a new publisher: %w", err)
	}

	reconnectCh, closeCh := chanManager.NotifyReconnect()
	publisher := &publisher{
		chanManager: 			chanManager,
		connManager: 			connection.connManager,
		reconnectErrCh:	 		reconnectCh,
		closeConnectionCh: 		closeCh,
		handlerMu:				&sync.RWMutex{},
		notifyReturnHandler: 	nil,
		notifyPublishHandler: 	nil,
		options:				*options,
	}
	
	err = publisher.start()
	if err != nil {
		return nil, fmt.Errorf("unable to start the publisher: %w", err)
	}

	if options.ConfirmMode {
		publisher.NotifyPublish(func(_ Confirmation) {
			// blank handler
		})
	}

	go func() {
		for err := range publisher.reconnectErrCh {
			publisher.options.logger.Info(fmt.Sprintf("successful publisher recovery from %v", err))
			err := publisher.start()
			if err != nil {
				publisher.options.logger.Fatal(fmt.Sprintf("error on startup for publisher after cancel/close: %v", err))
				publisher.options.logger.Fatal("publisher closing, unable to recover")
				return
			}
			publisher.startReturnHandler()
			publisher.startPublishHandler()
		}
	}()

	return publisher, nil
}

func (p *publisher) Publish(context context.Context, data []byte, routingkeys []string, opts ...func(*PublishOptions)) error {
	if len(data) > int(MaxMsgSize) {
		return errors.New("message size is too large - please contact Platform Engineering for support")
	}

	options := &PublishOptions{}
	for _, opt := range opts {
		opt(options)
	}
	if options.DeliveryMode == 0 {
		options.DeliveryMode = Transient
	}

	for _, routingKey := range routingkeys {
		msg := amqp.Publishing{}
		msg.AppId = options.AppID
		msg.Body = data
		msg.ContentEncoding = options.ContentEncoding
		msg.ContentType = options.ContentType
		msg.CorrelationId = options.CorrelationID
		msg.DeliveryMode = options.DeliveryMode
		msg.Expiration = options.Expiration
		msg.Headers = toAMQPTable(options.Headers)
		msg.MessageId = options.MessageID
		msg.Priority = options.Priority
		msg.ReplyTo = options.ReplyTo
		msg.Timestamp = options.Timestamp
		msg.Type = options.Type
		msg.UserId = options.UserID

		err := p.chanManager.PublishWithContext(context, options.Exchange, routingKey, options.Mandatory, options.Immediate, msg)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *publisher) PublishWithConfirm(context context.Context, data []byte, routingKeys []string, opts ...func(*PublishOptions)) (PublisherConfirmation, error) {
	options := &PublishOptions{}
	for _, opt := range opts {
		opt(options)
	}
	if options.DeliveryMode == 0 {
		options.DeliveryMode = Transient
	}

	var deferredConfirmations []*amqp.DeferredConfirmation
	for _, routingKey := range routingKeys {
		msg := amqp.Publishing{}
		msg.AppId = options.AppID
		msg.Body = data
		msg.ContentEncoding = options.ContentEncoding
		msg.ContentType = options.ContentType
		msg.CorrelationId = options.CorrelationID
		msg.DeliveryMode = options.DeliveryMode
		msg.Expiration = options.Expiration
		msg.Headers = toAMQPTable(options.Headers)
		msg.MessageId = options.MessageID
		msg.Priority = options.Priority
		msg.ReplyTo = options.ReplyTo
		msg.Timestamp = options.Timestamp
		msg.Type = options.Type
		msg.UserId = options.UserID

		confirmation, err := p.chanManager.PublishWithDeferredConfirmation(context, options.Exchange, routingKey, options.Mandatory, options.Immediate, msg)
		if err != nil {
			return nil, err
		}
		deferredConfirmations = append(deferredConfirmations, confirmation)
	}

	return deferredConfirmations, nil
}

func (p *publisher) Close() {
	err := p.chanManager.Close()
	if err != nil {
		p.options.logger.Warn(fmt.Sprintf("error while closing the channel: %v", err))
	}
	p.options.logger.Info("closing publisher")
	go func() {
		p.closeConnectionCh <- struct{}{}
	}()
}

func (p *publisher) NotifyReturn(handler func(r ReturnResponse)) {
	p.handlerMu.Lock()
	start := p.notifyReturnHandler == nil
	p.notifyReturnHandler = handler
	p.handlerMu.Unlock()

	if start {
		p.startReturnHandler()
	}
}

func (p *publisher) NotifyPublish(handler func(c Confirmation)) {
	p.handlerMu.Lock()
	start := p.notifyPublishHandler == nil
	p.notifyPublishHandler = handler
	p.handlerMu.Unlock()

	if start {
		p.startPublishHandler()
	}
}

func (p *publisher) start() error {
	err := declareExchange(p.chanManager, p.options.ExchangeOptions)
	if err != nil {
		return fmt.Errorf("unable to declare exchange: %w", err)
	}

	if p.options.ExchangeOptions.QueueOptions.Name != "" {
		err = declareQueue(p.chanManager, p.options.ExchangeOptions.QueueOptions)
		if err != nil {
			return fmt.Errorf("unable to declare queue: %w", err)
		}
	}

	if len(p.options.ExchangeOptions.Bindings) != 0 {
		err = declareBindings(
			p.chanManager, 
			p.options.ExchangeOptions.Name, 
			p.options.ExchangeOptions.QueueOptions.Name, 
			p.options.ExchangeOptions.Bindings)
		if err != nil {
			return fmt.Errorf("unable to declare binding: %w", err)
		}
	}

	return nil
}

func (p *publisher) startReturnHandler() {
	p.handlerMu.Lock()
	if p.notifyReturnHandler == nil {
		p.handlerMu.Unlock()
		return
	}
	p.handlerMu.Unlock()

	go func() {
		returns := p.chanManager.NotifyReturn(make(chan amqp.Return, 1))
		for r := range returns {
			go p.notifyReturnHandler(ReturnResponse{r})
		}
	}()
}

func (p *publisher) startPublishHandler() {
	p.handlerMu.Lock()
	if p.notifyPublishHandler == nil {
		p.handlerMu.Unlock()
		return
	}
	p.handlerMu.Unlock()

	go func() {
		confCh := p.chanManager.NotifyPublish(make(chan amqp.Confirmation, 1))
		for c := range confCh {
			go p.notifyPublishHandler(Confirmation{
				Confirmation: c,
				ReconnectionCount: int(p.chanManager.GetReconnectionCount()),
			})
		}
	}()
}

// WithPublishOptionsExchange sets the message publishing exchange target value
func WithPublishOptionsExchange(exchange string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Exchange = exchange
	}
}

// WithPublishOptionsMandatory sets the message publishing mandatory value
func WithPublishOptionsMandatory(options *PublishOptions) {
	options.Mandatory = true
}

// WithPublishOptionsImmediate sets the message publishing immediate value
func WithPublishOptionsImmediate(options *PublishOptions) {
	options.Immediate = true
}

// WithPublishOptionsContentType sets the message publishing content type value
func WithPublishOptionsContentType(contentType string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.ContentType = contentType
	}
}

// WithPublishOptionsPersistentDelivery sets the message publishing delivery mode to Persistent
func WithPublishOptionsPersistentDelivery(options *PublishOptions) {
	options.DeliveryMode = Persistent
}

// WithPublishOptionsExpiration sets the message publishing TTL expiration value
func WithPublishOptionsExpiration(expiration string) func(options *PublishOptions) {
	return func(options *PublishOptions) {
		options.Expiration = expiration
	}
}

// WithPublishOptionsHeaders sets the message publishing header value
func WithPublishOptionsHeaders(headers Table) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Headers = headers
	}
}

// WithPublishOptionsContentEncoding sets the message publishing content encoding value
func WithPublishOptionsContentEncoding(contentEncoding string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.ContentEncoding = contentEncoding
	}
}

// WithPublishOptionsPriority sets the message publishing priority value
func WithPublishOptionsPriority(priority uint8) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Priority = priority
	}
}

// WithPublishOptionsCorrelationID sets the message publishing correlation ID value
func WithPublishOptionsCorrelationID(correlationID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.CorrelationID = correlationID
	}
}


// WithPublishOptionsReplyTo sets the message publishing reply-to value
func WithPublishOptionsReplyTo(replyTo string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.ReplyTo = replyTo
	}
}

// WithPublishOptionsMessageID sets the message publishing message ID value
func WithPublishOptionsMessageID(messageID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.MessageID = messageID
	}
}


// WithPublishOptionsTimestamp sets the message publishing timestamp value
func WithPublishOptionsTimestamp(timestamp time.Time) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Timestamp = timestamp
	}
}


// WithPublishOptionsType sets the message publishing message type value
func WithPublishOptionsType(messageType string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Type = messageType
	}
}


// WithPublishOptionsUserID sets the message publishing user ID value
func WithPublishOptionsUserID(userID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.UserID = userID
	}
}

// WithPublishOptionsAppID sets the message publishing application ID value
func WithPublishOptionsAppID(appID string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.AppID = appID
	}
}

// WithPublisherOptionsExchangeName sets the publisher exchange name
func WithPublisherOptionsExchangeName(name string) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		options.ExchangeOptions.Name = name
	}
}

// WithPublisherOptionsExchangeKind sets the publisher exchange kind (FanOut, Direct, Topic)
func WithPublisherOptionsExchangeKind(kind string) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		options.ExchangeOptions.Kind = kind
	}
}

// WithPublisherOptionsExchangeDurable enables the publisher exchange durable flag
func WithPublisherOptionsExchangeDurable(options *PublisherOptions) {
	options.ExchangeOptions.Durable = true
}

// WithPublisherOptionsExchangeAutoDelete enables the publisher exchange auto-delete flag
func WithPublisherOptionsExchangeAutoDelete(options *PublisherOptions) {
	options.ExchangeOptions.AutoDelete = true
}


// WithPublisherOptionsExchangeInternal enables the publisher exchange internal flag
func WithPublisherOptionsExchangeInternal(options *PublisherOptions) {
	options.ExchangeOptions.Internal = true
}

// WithPublisherOptionsExchangeNoWait enables the publisher exchange no-wait flag
func WithPublisherOptionsExchangeNoWait(options *PublisherOptions) {
	options.ExchangeOptions.NoWait = true
}

// WithPublisherOptionsExchangeDeclare enables the publisher exchange declare flag
func WithPublisherOptionsExchangeDeclare(options *PublisherOptions) {
	options.ExchangeOptions.Declare = true
}

// WithPublisherOptionsExchangePassive enables the publisher exchange passive flag
func WithPublisherOptionsExchangePassive(options *PublisherOptions) {
	options.ExchangeOptions.Passive = true
}

// WithPublisherOptionsExchangeArgs sets the publisher exchange AMQP configuration value
func WithPublisherOptionsExchangeArgs(args Table) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		options.ExchangeOptions.Args = args
	}
}

// WithPublisherOptionsExchangeQueueOptions sets the publisher exchange queue options
func WithPublisherOptionsExchangeQueueOptions(opts ...func(*QueueOptions)) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		queueOptions := &QueueOptions{}
		for _, opt := range opts {
			opt(queueOptions)
		}
		options.ExchangeOptions.QueueOptions = *queueOptions
	}
}

// WithPublisherOptionsExchangeBindings sets the publisher exchange bindings
func WithPublisherOptionsExchangeBindings(bindings []Binding) func(*PublisherOptions) {
	return func(options *PublisherOptions) {
		options.ExchangeOptions.Bindings = bindings
	}
}

func getDefaultPublisherOptions() PublisherOptions {
	return PublisherOptions{
		ExchangeOptions: ExchangeOptions{
			Name: 			"",
			Kind: 			amqp.ExchangeDirect,
			Durable:		false,
			AutoDelete: 	false,
			Internal:		false,
			NoWait:			false,
			Passive:		false,
			Args:			Table{},
			Declare:		false,
		},
		logger: log.Logger,
		ConfirmMode: false,
	}
}
