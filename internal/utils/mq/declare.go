package mq

func declareExchange(channelManager *channelManager, options ExchangeOptions) error {
	if !options.Declare {
		return nil
	}
	if options.Passive {
		return channelManager.ExchangeDeclarePassive(
			options.Name,
			options.Kind,
			options.Durable,
			options.AutoDelete,
			options.Internal,
			options.NoWait,
			toAMQPTable(options.Args),
		)
	}
	
	return channelManager.ExchangeDeclare(
			options.Name,
			options.Kind,
			options.Durable,
			options.AutoDelete,
			options.Internal,
			options.NoWait,
			toAMQPTable(options.Args),
	)
}

func declareQueue(channelManager *channelManager, options QueueOptions) error {
	if !options.Declare {
		return nil
	}
	if options.Passive {
		_, err := channelManager.QueueDeclarePassive(
			options.Name, 
			options.Durable, 
			options.AutoDelete, 
			options.Exclusive, 
			options.NoWait, 
			toAMQPTable(options.Args))
		return err
	}

	_, err := channelManager.QueueDeclare(
		options.Name, 
		options.Durable, 
		options.AutoDelete, 
		options.Exclusive, 
		options.NoWait, 
		toAMQPTable(options.Args))
	return err
}

func declareBindings(channelManager *channelManager, exchange string, queue string, bindings []Binding) error {
	for _, binding := range bindings {
		if !binding.Options.Declare {
			continue
		}
		err := channelManager.QueueBind(
			queue,
			binding.RoutingKey,
			exchange,
			binding.Options.NoWait,
			toAMQPTable(binding.Options.Args),
		)
		if err != nil {
			return err
		}
	}

	return nil
}