namespace EventHubsDemo
{
    using System;
    using System.Diagnostics;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;
    using System.Collections.Generic;
    using Microsoft.ServiceBus.Messaging;
    using CloudStorageAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount;

    public class EventObjEventProcessor : IEventProcessor
    {
        Stopwatch checkpointStopWatch;

        public async Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.WriteLine($"Event processor is closing for partition {context.Lease.PartitionId} with reason: {reason}.");

            if (reason == CloseReason.Shutdown)
            {
                await context.CheckpointAsync();
            }
        }

        public Task OpenAsync(PartitionContext context)
        {
            Console.WriteLine($"Event processor is opening for partition {context.Lease.PartitionId} with offset: {context.Lease.Offset}.");

            this.checkpointStopWatch = new Stopwatch();
            this.checkpointStopWatch.Start();

            return Task.FromResult<object>(null);
        }

        public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (EventData eventData in messages)
            {
                string data = Encoding.UTF8.GetString(eventData.GetBytes());
                Console.WriteLine($"Message received: {data} in partition: {context.Lease.PartitionId}.");

                EventObj @event = JsonConvert.DeserializeObject<EventObj>(data);
                Console.WriteLine("Received Event:");
                Console.WriteLine($"\t Event Name: {@event.EventName}");
                Console.WriteLine($"\t Event Number: {@event.EventNumber}");
                Console.WriteLine($"\t Event Id: {@event.EventId}");
                Console.WriteLine($"\t Event Insertion Time: {@event.InsertionTime}");

                InsertEntityToAzureTable(@event);
            }
            return Task.FromResult<object>(null);
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

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Consumer.StorageConnectionStringForEventProcessor);
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
