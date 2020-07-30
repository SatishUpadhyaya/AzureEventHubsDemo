namespace EventHubsDemo
{
    using System;

    /// <summary>
    /// Event Obj class
    /// </summary>
    public class EventObj
    {
        public EventObj()
        {
        }

        public string EventName { get; set; }

        public int EventNumber { get; set; }

        public string EventId { get; set; }

        public DateTime InsertionTime { get; set; }
    }
}
