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

type channelManager struct {
	logger 						*zap.Logger
	channel 					*amqp.Channel
	connManager 				*connectionManager
	channelMu 					*sync.RWMutex
	reconnectInterval			time.Duration
	reconnectCount 				uint
	reconnectCountMu 			*sync.Mutex
	dispatcher 					*dispatcher
}

func newChannelManager(connManager *connectionManager, reconnectInterval time.Duration) (*channelManager, error) {
	ch, err := getNewChannel(connManager)
	if err != nil {
		return nil, fmt.Errorf("unable to create a new channel: %w", err)
	}

	chanManager := channelManager{
		logger:					log.Logger,
		connManager:			connManager,
		channel: 				ch,
		channelMu: 				&sync.RWMutex{},
		reconnectInterval: 		reconnectInterval,
		reconnectCount: 		0,
		dispatcher: 			newDispatcher(),
	}

	go chanManager.startNotifyCancelClose()
	return &chanManager, nil
}

func (c *channelManager) ExchangeDeclare(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table) error {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()

	return c.channel.ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
}

func (c *channelManager) ExchangeDeclarePassive(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table) error {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()

	return c.channel.ExchangeDeclarePassive(name, kind, durable, autoDelete, internal, noWait, args)
}

func (c *channelManager) Qos(prefetchCount int, prefetchSize int, global bool) error {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()

	return c.channel.Qos(prefetchCount, prefetchSize, global)
}

func (c *channelManager) QueueDeclare(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table) (amqp.Queue, error) {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()

	return c.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
}

func (c *channelManager) QueueDeclarePassive(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table) (amqp.Queue, error) {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()

	return c.channel.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
}

func (c *channelManager) QueueBind(name string, key string, exchange string, noWait bool, args amqp.Table) error {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()

	return c.channel.QueueBind(name, key, exchange, noWait, args)
}

func (c *channelManager) PublishWithContext(context context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) error {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()

	return c.channel.PublishWithContext(context, exchange, key, mandatory, immediate, msg)
}

func (c *channelManager) PublishWithDeferredConfirmation(context context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()

	return c.channel.PublishWithDeferredConfirmWithContext(context, exchange, key, mandatory, immediate, msg)
}

func (c *channelManager) Confirm(noWait bool) error {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()

	return c.channel.Confirm(noWait)
}

func (c *channelManager) Consume(queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()

	return c.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (c *channelManager) NotifyReturn(r chan amqp.Return) chan amqp.Return {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()

	return c.channel.NotifyReturn(r)
}

func (c *channelManager) NotifyPublish(f chan amqp.Confirmation) chan amqp.Confirmation {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()

	return c.channel.NotifyPublish(f)
}

func (c *channelManager) Close() error {
	c.logger.Info("closing the channel manager")
	c.channelMu.Lock()
	defer c.channelMu.Unlock()

	return c.channel.Close()
}

func (c *channelManager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return c.dispatcher.Add()
}

func (c *channelManager) GetReconnectionCount() uint {
	c.reconnectCountMu.Lock()
	defer c.reconnectCountMu.Unlock()
	return c.reconnectCount
}

func (c *channelManager) startNotifyCancelClose() {
	closeCh := c.channel.NotifyClose(make(chan *amqp.Error, 1))
	cancelCh := c.channel.NotifyCancel(make(chan string, 1))

	select {
	case err := <-closeCh:
		if err != nil {
			c.logger.Error(fmt.Sprintf("attempting to reconnect to AMQP server after close with error: %v", err))
			c.reconnectLoop()
			c.logger.Warn("successfully reconnected to AMQP server")
			c.dispatcher.Dispatch(err)
		}
		if err == nil {
			c.logger.Info("AMQP channel closed gracefully")
		}
	case err := <-cancelCh:
		c.logger.Error(fmt.Sprintf("attempting to reconnect to AMQP server after cancel with error: %v", err))
		c.reconnectLoop()
		c.logger.Warn("successfully reconnected to AMQP server")
		c.dispatcher.Dispatch(errors.New(err))
	}
}

func (c *channelManager) reconnectLoop() {
	for {
		c.logger.Info(fmt.Sprintf("waiting %s seconds to attempt to reconnect to the AMQP server", c.reconnectInterval))
		time.Sleep(c.reconnectInterval)
		err := c.reconnect()
		if err != nil {
			c.logger.Error(fmt.Sprintf("error reconnecting to AMQP server: %v", err))
		} else {
			c.incrementReconnectionCount()
			go c.startNotifyCancelClose()
			return
		}
	}
}

func (c *channelManager) reconnect() error {
	c.channelMu.Lock()
	defer c.channelMu.Unlock()

	newChannel, err := getNewChannel(c.connManager)
	if err != nil {
		return err
	}

	if err = c.channel.Close(); err != nil {
		c.logger.Warn(fmt.Sprintf("error closing channel while reconnecting: %v", err))
	}

	c.channel = newChannel
	return nil
}

func (c *channelManager) incrementReconnectionCount() {
	c.reconnectCountMu.Lock()
	defer c.reconnectCountMu.Unlock()
	c.reconnectCount++
}

func getNewChannel(connManager *connectionManager) (*amqp.Channel, error) {
	conn := connManager.LockConnection()
	defer connManager.UnlockConnection()

	return conn.Channel()
}