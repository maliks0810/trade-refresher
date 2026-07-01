package mq

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"refresher/trade-refresher/internal/utils/log"
)

type dispatcher struct {
	logger 			*zap.Logger
	subscribers 	map[string]dispatcherSubscriber
	subscribersMu 	*sync.Mutex
}

type dispatcherSubscriber struct {
	notifyCancelCloseChan 	chan error
	closeCh 				<-chan struct{}
}

func newDispatcher() *dispatcher {
	return &dispatcher{
		logger:			log.Logger,
		subscribers: 	make(map[string]dispatcherSubscriber),
		subscribersMu: 	&sync.Mutex{},
	}
}

func (d *dispatcher) Add() (<-chan error, chan<- struct{}) {
	id := uuid.NewString()

	closeCh := make(chan struct{})
	notifyCancelCloseChan := make(chan error)
	d.subscribersMu.Lock()
	d.subscribers[id] = dispatcherSubscriber{
		notifyCancelCloseChan: notifyCancelCloseChan,
		closeCh: closeCh,
	}
	d.subscribersMu.Unlock()

	go func(id string) {
		<-closeCh
		d.subscribersMu.Lock()
		defer d.subscribersMu.Unlock()
		sub, ok := d.subscribers[id]
		if !ok {
			return	
		}
		close(sub.notifyCancelCloseChan)
		delete(d.subscribers, id)
	}(id)

	return notifyCancelCloseChan, closeCh
}

func (d *dispatcher) Dispatch(err error) error {
	d.subscribersMu.Lock()
	defer d.subscribersMu.Unlock()

	for _, subscriber := range d.subscribers {
		select {
		case <-time.After(time.Second *5):
			d.logger.Error("unexpected AMQP error: timeout in dispatch")
		case subscriber.notifyCancelCloseChan <- err:
			d.logger.Error(fmt.Sprintf("error received by Dispatcher: %v", err))
		}
	}

	return nil
}