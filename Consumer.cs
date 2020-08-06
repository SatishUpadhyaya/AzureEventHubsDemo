namespace EventHubsDemo
{
    using System;
    using System.Text;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage;
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

        public void ConsumerStart()
        {
            string consumerGroupName = "test";

            EventHubClient client = EventHubClient.CreateFromConnectionString(EventHubConnectionString, EventHubName);
            EventHubConsumerGroup consumerGroup = client.GetConsumerGroup(consumerGroupName);
            EventHubReceiver receiver = consumerGroup.CreateReceiver(client.GetRuntimeInformation().PartitionIds[0]);

            while (true)
            {
                Microsoft.ServiceBus.Messaging.EventData eventData = receiver.ReceiveAsync().Result;

                try
                {
                    string data = Encoding.UTF8.GetString(eventData.GetBytes());
                    Console.WriteLine($"Message received: {data}");

                    EventObj @event = JsonConvert.DeserializeObject<EventObj>(data);
                    Console.WriteLine("Received Event:");
                    Console.WriteLine($"\t Event Name: {@event.EventName}");
                    Console.WriteLine($"\t Event Number: {@event.EventNumber}");
                    Console.WriteLine($"\t Event Id: {@event.EventId}");
                    Console.WriteLine($"\t Event Insertion Time: {@event.InsertionTime}");

                    InsertEntityToAzureTable(@event);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Exception occured while processing event: {ex}");
                }
            }
        }

        public void InsertEntityToAzureTable(EventObj @event)
        {
            Console.WriteLine("Inserting entities to azure table ...");
            try
            {
                Dictionary<string, EntityProperty> entities = new Dictionary<string, EntityProperty>()
                {
                    { "EventName", new EntityProperty(@event.EventName) },
                    { "EventNumber", new EntityProperty(@event.EventNumber) },
                    { "EventId", new EntityProperty(@event.EventId) },
                };

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
                CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

                string eventNameNoSpace = @event.EventName.Replace(" ", String.Empty);
                DynamicTableEntity entity = new DynamicTableEntity(
                    eventNameNoSpace,
                    @event.EventId,
                    "*",
                    entities);

                CloudTable cloudTable = tableClient.GetTableReference("newtable");
                cloudTable.CreateIfNotExists();
                cloudTable.Execute(TableOperation.InsertOrReplace(entity));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occured while creating a table entity: {ex}");
            }
        }
    }
}
