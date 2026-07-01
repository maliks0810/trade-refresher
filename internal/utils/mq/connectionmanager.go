package mq

import (
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type connectionManager struct {
	url 						string
	logger 						*zap.Logger
	connection 					*amqp.Connection
	amqpConfig					amqp.Config
	connectionMu 				*sync.RWMutex
	ReconnectInterval 			time.Duration
	reconnectionCount			uint
	reconnectionCountMu			*sync.Mutex
	dispatcher 					*dispatcher
}

func newConnectionManager(connectionUrl string, config amqp.Config, logger *zap.Logger, reconnectInterval time.Duration) (*connectionManager, error) {
	conn, err := dial(connectionUrl)
	if err != nil {
		return nil, err
	}

	mgr := connectionManager{
		url: 					connectionUrl,
		logger: 				logger,
		connection: 			conn,
		amqpConfig: 			config,
		connectionMu: 			&sync.RWMutex{},
		ReconnectInterval:		reconnectInterval,
		reconnectionCount: 		0,
		reconnectionCountMu: 	&sync.Mutex{},
		dispatcher: 			newDispatcher(),		
	}

	go mgr.startNotifyOnClose();

	return &mgr, nil
}

func (c *connectionManager) Close() error {
	c.connectionMu.Lock()
	defer c.connectionMu.Unlock()

	return c.connection.Close()
}

func (c *connectionManager) IsClosed() bool {
	c.connectionMu.Lock()
	defer c.connectionMu.Unlock()
	return c.connection.IsClosed()
}

func (c *connectionManager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return c.dispatcher.Add()
}

func (c *connectionManager) GetReconnectionCount() uint {
	c.reconnectionCountMu.Lock()
	defer c.reconnectionCountMu.Unlock()
	return c.reconnectionCount
}

func (c *connectionManager) LockConnection() *amqp.Connection {
	c.connectionMu.RLock()
	return c.connection
}

func (c *connectionManager) UnlockConnection() {
	c.connectionMu.RUnlock()
}

func (c *connectionManager) incrementReconnectionCount() {
	c.reconnectionCountMu.Lock()
	defer c.reconnectionCountMu.Unlock()
	c.reconnectionCount++
}

func (c *connectionManager) startNotifyOnClose() {
	notifyCloseCh := c.connection.NotifyClose(make(chan *amqp.Error, 1))

	err := <-notifyCloseCh
	if err != nil {
		c.reconnectLoop()
	}
}

func (c *connectionManager) reconnectLoop() {
	for {
		time.Sleep(c.ReconnectInterval)
		err := c.reconnect()
		if err == nil {
			c.incrementReconnectionCount()
			go c.startNotifyOnClose()
			return
		}
	}
}

func (c *connectionManager) reconnect() error {
	c.connectionMu.Lock()
	defer c.connectionMu.Unlock()

	conn, err := dial(c.url)
	if err != nil {
		return err
	}

	c.connection = conn
	return nil
}

func dial(url string) (*amqp.Connection, error) {
	if url == "" {
		return nil, errors.New("invalid connection URL provided - cannot be empty")
	}

	return amqp.Dial(url)
}

