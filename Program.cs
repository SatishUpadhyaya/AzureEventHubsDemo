namespace EventHubsDemo
{
    using System;
    using System.Configuration;

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

        public void StartConsumer()
        {
            Consumer consumer = new Consumer()
            {
                EventHubName = eventHubName,
                EventHubConnectionString = eventHubConnectionString,
                StorageConnectionString = storageConnectionString,
            };
            consumer.ConsumerStart();
        }
    }

    /// <summary>
    /// Main Class
    /// </summary>
    public class Program
    {
        public static void Main()
        {
            Console.WriteLine("Starting Program ... \n Press 'ENTER' to continue ...");
            Console.ReadLine();

            Tester tester = new Tester();

            tester.StartProducer();

            Console.WriteLine("Press 'ENTER' to continue ...");
            Console.ReadLine();

            tester.StartConsumer();

            Console.WriteLine("Done!");
            Console.ReadLine();
        }
    }
}
