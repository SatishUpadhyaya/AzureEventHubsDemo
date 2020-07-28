namespace EventHubsDemo
{
    using System;
    using System.Text;
    using System.Threading.Tasks;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Consumer;
    using Azure.Messaging.EventHubs.Processor;
    using Azure.Storage.Blobs;

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
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            BlobServiceClient blobServiceClient = new BlobServiceClient(BlobStorageConnectionString);
            // string containerName = "Blobs" + Guid.NewGuid().ToString();
            string containerName = "default";
            BlobContainerClient storageClient = blobServiceClient.CreateBlobContainer(containerName);

            EventProcessorClient processor = new EventProcessorClient(storageClient, consumerGroup, EventHubConnectionString);

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
            Console.WriteLine($"Received Event: {eventBody}");

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
