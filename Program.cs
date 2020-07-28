namespace EventHubsDemo
{
    using System;
    using System.Configuration;
    using System.Threading.Tasks;

    /// <summary>
    /// Tester class
    /// </summary>
    public class Tester
    {
        public readonly string eventHubName = ConfigurationManager.AppSettings["EventHubName"];

        public readonly string eventHubConnectionString = ConfigurationManager.AppSettings["EventHubConnectionString"];

        public readonly string storageConnectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

        public Tester()
        {
        }

        public void StartProducer()
        {
            Producer producer = new Producer()
            {
                EventHubName = eventHubName,
                EventHubConnectionString = eventHubConnectionString,
            };
            producer.ProducerStart();
        }

        public async Task StartConsumer()
        {
            Consumer consumer = new Consumer()
            {
                EventHubName = eventHubName,
                EventHubConnectionString = eventHubConnectionString,
                BlobStorageConnectionString = storageConnectionString,
            };
            await consumer.ConsumerStart();
        }
    }

    /// <summary>
    /// Main Class
    /// </summary>
    public class Program
    {
        public static async Task Main()
        {
            Tester tester = new Tester();

            tester.StartProducer();

            Console.WriteLine("Press 'ENTER' to continue ...");
            Console.ReadLine();

            await tester.StartConsumer();

            Console.WriteLine("Done!");
            Console.ReadLine();
        }
    }
}
