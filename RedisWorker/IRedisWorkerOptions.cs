using System;

namespace RedisWorker
{
    public interface IRedisWorkerOptions
    {
        /// <summary>
        /// Whether to save completed work in an audit hash
        /// Name of hash is determined by IRedisWorkerNamingStrategy.HashName
        /// </summary>
        bool Audit { get; set; }

        /// <summary>
        /// How many times to retry work on exception before sending to error hash
        /// Name of hash is determined by IRedisWorkerNamingStrategy.ErrorName
        /// </summary>
        int Retries { get; set; }

        /// <summary>
        /// How many messages to process simultaneously for this worker
        /// </summary>
        int MaxDegreeOfParallelism { get; set; }

        /// <summary>
        /// Amount of time before work is considered orphaned
        /// Work in processing list older than this amount will be placed back onto the pending queue
        /// </summary>
        TimeSpan ProcessingGracePeriod { get; set; }

        /// <summary>
        /// How often to check for orphaned in progress work
        /// </summary>
        TimeSpan OrphanedInProcessInterval { get; set; }

        /// <summary>
        /// Used to determine names of lists and hashes where data is stored
        /// </summary>
        IRedisWorkerNamingStrategy NamingStrategy { get; set; }
    }
}