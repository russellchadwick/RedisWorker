using System;

namespace RedisWorker
{
    public class RedisWork<TWork>
    {
        public DateTime WhenQueued { get; set; }
        public DateTime WhenCompleted { get; set; }
        public DateTime WhenErrored { get; set; }
        public int RetryCount { get; set; }
        public TWork Work { get; set; }
    }
}