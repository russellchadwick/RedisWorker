using System;

namespace RedisWorker
{
    public class DefaultRedisWorkerOptions<TWork> : IRedisWorkerOptions
    {
        public bool Audit { get; set; }
        public int Retries { get; set; }
        public int MaxDegreeOfParallelism { get; set; }
        public TimeSpan ProcessingGracePeriod { get; set; }
        public TimeSpan OrphanedInProcessInterval { get; set; }
        public IRedisWorkerNamingStrategy NamingStrategy { get; set; }

        public DefaultRedisWorkerOptions()
        {
            Audit = true;
            Retries = 3;
            MaxDegreeOfParallelism = 5;
            ProcessingGracePeriod = new TimeSpan(0, 5, 0);
            OrphanedInProcessInterval = new TimeSpan(0, 5, 0);
            NamingStrategy = new DefaultRedisWorkerNamingStrategy(typeof (TWork).Name);
        }
    }
}