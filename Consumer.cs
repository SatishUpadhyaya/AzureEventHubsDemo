namespace EventHubsDemo
{
    using System;
    using System.Text;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Consumer;
    using Azure.Messaging.EventHubs.Processor;
    using Azure.Storage.Blobs;
    using Newtonsoft.Json;

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
            string containerName = "blob" + Guid.NewGuid().ToString();
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
            string eventBody = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());
            EventObj @event = JsonConvert.DeserializeObject<EventObj>(eventBody);
            Console.WriteLine("Received Event:");
            Console.WriteLine($"\t Event Name: {@event.EventName}");
            Console.WriteLine($"\t Event Number: {@event.EventNumber}");
            Console.WriteLine($"\t Event Id: {@event.EventId}");

            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        public Task ProcessErrorHandler(ProcessErrorEventArgs errorArgs)
        {
            Console.WriteLine($"Partition '{errorArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine($"Exception: {errorArgs.Exception.Message}");
            return Task.CompletedTask;
        }
    }
}
