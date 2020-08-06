namespace EventHubsDemo
{
    using System;
    using Microsoft.ServiceBus.Messaging;

    /// <summary>
    /// Consumer Class
    /// </summary>
    public class Consumer
    {
        public Consumer()
        {
            Console.WriteLine($"Hello from {nameof(Consumer)}!");
        }

        public string EventHubName { get; set; }

        public string EventHubConnectionString { get; set; }

        public string StorageConnectionString { get; set; }

        public static string StorageConnectionStringForEventProcessor;

        public void ConsumerStart()
        {
            StorageConnectionStringForEventProcessor = StorageConnectionString;
            string consumerGroupName = "test";
            string eventProcessorHostName = Guid.NewGuid().ToString();

            EventProcessorHost eventProcessorHost = new EventProcessorHost(
                eventProcessorHostName,
                EventHubName,
                consumerGroupName,
                EventHubConnectionString,
                StorageConnectionString);
            Console.WriteLine("EventProcessor is starting ...");

            EventProcessorOptions eventProcessorOptions = new EventProcessorOptions();
            eventProcessorOptions.ExceptionReceived += (obj, ex) => { Console.WriteLine($"Exception occured while receiving incident with exception: {ex.Exception.ToString()}"); };
            eventProcessorHost.RegisterEventProcessorAsync<EventObjEventProcessor>(eventProcessorOptions).Wait();

            Console.WriteLine("Press 'ENTER' to continue ...");
            Console.ReadLine();
            eventProcessorHost.UnregisterEventProcessorAsync().Wait();
        }
    }
}
