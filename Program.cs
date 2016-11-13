using System;
using Microsoft.ServiceBus.Messaging;
using System.Configuration;

namespace CloudMessageProcessor
{
    class Program
    {
        static string iotHubD2cEndpoint = null;
        static string storageConnectionString = null;
        static string iotHubconnectionString = null;

        static void Main(string[] args)
        {
            storageConnectionString = ConfigurationManager.AppSettings["StorageConnectionString"].ToString();
            iotHubconnectionString = ConfigurationManager.AppSettings["IoTHubConnectionString"].ToString();
            iotHubD2cEndpoint = ConfigurationManager.AppSettings["IoTHubD2cEndpoint"].ToString();

            string eventProcessorHostName = Guid.NewGuid().ToString();

            //Initializes a new instance of the Microsoft.ServiceBus.Messaging.EventProcessorHost
            EventProcessorHost eventProcessorHost = new EventProcessorHost(eventProcessorHostName, iotHubD2cEndpoint, EventHubConsumerGroup.DefaultGroupName, 
                                                                            iotHubconnectionString, storageConnectionString, "messages-events");
            Console.WriteLine("Registering EventProcessor...");

            eventProcessorHost.RegisterEventProcessorAsync<IOTEventProcessor>().Wait();

            Console.WriteLine("Receiving. Press enter key to stop worker.");
            Console.ReadLine();
            eventProcessorHost.UnregisterEventProcessorAsync().Wait();
        }
    }
}
