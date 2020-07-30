namespace EventHubsDemo
{
    using System;
    using System.Text;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Processor;
    using Microsoft.WindowsAzure.Storage.Table;
    using Azure.Storage.Blobs;
    using Newtonsoft.Json;
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage;
    using System.Configuration;

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

        public string BlobStorageConnectionString { get; set; }

        public async Task ConsumerStart()
        {
            string consumerGroup = "test";

            BlobServiceClient blobServiceClient = new BlobServiceClient(BlobStorageConnectionString);
            string containerName = "blob-" + Guid.NewGuid().ToString();
            BlobContainerClient storageClient = blobServiceClient.CreateBlobContainer(containerName);

            EventProcessorClient processor = new EventProcessorClient(storageClient, consumerGroup, EventHubConnectionString, EventHubName);

            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            Console.WriteLine("Started to process event/error ...");

            await processor.StartProcessingAsync();

            await Task.Delay(TimeSpan.FromSeconds(10));

            await processor.StopProcessingAsync();
        }

        public async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            try
            {
                string eventBody = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());
                EventObj @event = JsonConvert.DeserializeObject<EventObj>(eventBody);
                Console.WriteLine("Received Event:");
                Console.WriteLine($"\t Event Name: {@event.EventName}");
                Console.WriteLine($"\t Event Number: {@event.EventNumber}");
                Console.WriteLine($"\t Event Id: {@event.EventId}");
                Console.WriteLine($"\t Event Insertion Time: {@event.InsertionTime}");

                InsertEntityToAzureTable(@event);

                await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occured while processing event: {ex}");
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

                string connectionString = ConfigurationManager.AppSettings["StorageTableConnectionString"];
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
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

        public Task ProcessErrorHandler(ProcessErrorEventArgs errorArgs)
        {
            Console.WriteLine($"Partition '{errorArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine($"Exception: {errorArgs.Exception.Message}");
            return Task.CompletedTask;
        }
    }
}
