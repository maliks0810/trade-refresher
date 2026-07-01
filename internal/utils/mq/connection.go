package mq

import (
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"refresher/trade-refresher/internal/utils/log"
)

type connection struct {
	connManager			*connectionManager
	reconnectErrorCh	<-chan error
	closeConnectionCh 	chan<- struct{}
	options 			ConnectionOptions
}

type ConnectionOptions struct {
	ReconnectInterval 	time.Duration
	Logger 				*zap.Logger
	Config 				amqp.Config
}

func newConnection(connstr string, opts ...func(*ConnectionOptions)) (*connection, error) {
	if connstr == "" {
		return nil, errors.New("connection string for AMQP services not set")
	}
	
	defaultOptions := defaultConnectionOptions()
	options := &defaultOptions
	for _, opt := range opts {
		opt(options)
	}

	manager, err := newConnectionManager(connstr, options.Config, options.Logger, options.ReconnectInterval)
	if err != nil {
		return nil, err
	}
	reconnectErrCh, closeCh := manager.NotifyReconnect()
	conn := &connection{
		connManager: manager,
		reconnectErrorCh: reconnectErrCh,
		closeConnectionCh: closeCh,
		options: *options,
	}

	go conn.handleRestarts()
	return conn, nil
}

func (c *connection) Close() error {
	c.closeConnectionCh <- struct{}{}
	return c.connManager.Close()
}

func (c *connection) IsClosed() bool {
	return c.connManager.IsClosed()
}

func (c *connection) handleRestarts() {
	for err := range c.reconnectErrorCh {
		c.options.Logger.Info(fmt.Sprintf("successful connection recovery from: %v", err))
	}
}

func defaultConnectionOptions() ConnectionOptions {
	return ConnectionOptions{
		ReconnectInterval: 		time.Second * 5,
		Logger: 				log.Logger,
		Config:					amqp.Config{},
	}
}
