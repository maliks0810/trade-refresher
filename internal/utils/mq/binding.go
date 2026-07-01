package mq

import (

)

// BindingOptions provides custom configuration options for exchange/queue bindings
type BindingOptions struct {
	NoWait 		bool			// When true, the binding will be assumed to be declared
	Args 		Table			// Additional custom arguments for the binding configuration
	Declare 	bool			// When true, will automatically declare the binding
}

type Binding struct {
	RoutingKey 	string			// The unique queue/topic name the exchange will route messages to
	Options 	BindingOptions	// Additional binding configuration options
}

func getDefaultBindingOptions() BindingOptions {
	return BindingOptions{
		NoWait:  false,
		Args:    Table{},
		Declare: true,
	}
}