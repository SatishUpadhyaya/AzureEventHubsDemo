namespace EventHubsDemo
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

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
    }
}
