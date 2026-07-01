package mq

// QueueOptions provides custom configurations for AMQP queues
type QueueOptions struct {
	Name 		string			// Unique name of the queue
	Durable 	bool			// When true, the queue will survive server restarts
	AutoDelete 	bool			// When true, the queue will be deleted (but not undeclared) after a period of time when all connections are closed
	Exclusive	bool			// When true, only accessible by the connection that declares the queue
	NoWait 		bool 			// When true, the queue will be assumed to be declared
	Passive 	bool			// When true, the queue is already assumed to exist
	Args 		Table			// Additional custom arguments for the queue configuration
	Declare 	bool			// When true, will automatically declare the queue with the AMQP service
}

// WithQueueOptionsName sets the AMQP queue name
func WithQueueOptionsName(name string) func(*QueueOptions) {
	return func(options *QueueOptions) {
		options.Name = name
	}
}

// WithQueueOptionsDurable sets the AMQP queue durable value
func WithQueueOptionsDurable(durable bool) func(*QueueOptions) {
	return func(options *QueueOptions) {
		options.Durable = durable
	}
}

// WithQueueOptionsAutoDelete sets the AMQP queue auto-delete value
func WithQueueOptionsAutoDelete(autoDelete bool) func(*QueueOptions) {
	return func(options *QueueOptions) {
		options.AutoDelete = autoDelete
	}
}

// WithQueueOptionsExclusive sets the AMQP exclusive value
func WithQueueOptionsExclusive(exclusive bool) func(*QueueOptions) {
	return func(options *QueueOptions) {
		options.Exclusive = exclusive
	}
}

// WithQueueOptionsNoWait sets the AMQP queue no-wait value
func WithQueueOptionsNoWait(noWait bool) func(*QueueOptions) {
	return func(options *QueueOptions) {
		options.NoWait = noWait
	}
}

// WithQueueOptionsPassive sets the AMQP passive value
func WithQueueOptionsPassive(passive bool) func(*QueueOptions) {
	return func(options *QueueOptions) {
		options.Passive = passive
	}
}

// WithQueueOptionsArgs sets the AMQP additional arguments value
func WithQueueOptionsArgs(args Table) func(*QueueOptions) {
	return func(options *QueueOptions) {
		options.Args = args
	}
}

// WithQueueOptionsDeclare sets the AMQP declare value
func WithQueueOptionsDeclare(declare bool) func(*QueueOptions) {
	return func(options *QueueOptions) {
		options.Declare = declare
	}
}