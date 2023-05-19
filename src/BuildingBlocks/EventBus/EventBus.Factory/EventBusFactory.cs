using EventBus.AzureServiceBus;
using EventBus.Base;
using EventBus.Base.Abstraction;
using EventBus.RabbitMQ;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventBus.Factory
{
    public class EventBusFactory
    {
        public static IEventBus Create(EventBusConfig eventBusConfig, IServiceProvider serviceProvider)
        {

            return eventBusConfig.EventBusType switch
            {
                EventBusType.AzureServiceBus => new EventBusServiceBus(serviceProvider, eventBusConfig ),
                _ => new EventBusRabbitMQ( eventBusConfig, serviceProvider),
            };
        }
    }
}
