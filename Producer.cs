namespace EventHubsDemo
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Azure.Messaging.EventHubs;
    using Azure.Messaging.EventHubs.Producer;
    using Newtonsoft.Json;

    /// <summary>
    /// Producer class
    /// </summary>
    public class Producer
    {
        public Producer()
        {
            Console.WriteLine($"Hello from {nameof(Producer)}!");
        }

        public string EventHubName { get; set; }

        public string EventHubConnectionString { get; set; }

        public async void ProducerStart()
        {
            List<string> eventData = CreateEvents();

            await using (var producerClient = new EventHubProducerClient(EventHubConnectionString, EventHubName))
            {
                using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

                foreach (string eData in eventData)
                {
                    eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(eData)));
                }

                await producerClient.SendAsync(eventBatch);
                Console.WriteLine($"A batch of {eventData.Count} events have been published.");
            }
        }

        public List<string> CreateEvents()
        {
            List<string> eventData = new List<string>();

            List<string> events = new List<string>()
            {
                "First", "Second", "Third",
            };

            int counter = 0;
            foreach (string e in events)
            {
                EventObj @event = new EventObj()
                {
                    EventName = e + " Event",
                    EventNumber = counter,
                    EventId = Guid.NewGuid().ToString(),
                };
                string eventStr = JsonConvert.SerializeObject(@event);
                eventData.Add(eventStr);
                counter += 1;
            }

            return eventData;
        }
    }
}
