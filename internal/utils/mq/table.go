package mq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Table map[string]interface{}

func toAMQPTable(table Table) amqp.Table {
	n := amqp.Table{}
	for k, v := range table {
		n[k] = v
	}
	return n
}