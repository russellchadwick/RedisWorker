using System;
using System.Threading.Tasks;
using BookSleeve;
using Newtonsoft.Json;

namespace RedisWorker
{
    public static class RedisWorkerQueuer
    {
        public static void QueueWork<TWork>(this RedisConnection redisConnection, TWork work, IRedisWorkerNamingStrategy redisWorkerNamingStrategy)
        {
            var workId = Guid.NewGuid().ToString();

            using (var redisTransaction = redisConnection.CreateTransaction())
            {
                var redisWork = new RedisWork<TWork>
                    {
                        WhenQueued = DateTime.UtcNow,
                        RetryCount = 0,
                        Work = work
                    };

                var serializedRedisWork = JsonConvert.SerializeObject(redisWork);

                redisTransaction.Lists.AddLast(0, redisWorkerNamingStrategy.QueueName, workId);
                redisTransaction.Hashes.Set(0, redisWorkerNamingStrategy.WorkName, workId, serializedRedisWork);

                redisTransaction.Execute();
            }
        }

        public static void QueueWork<TWork>(this RedisConnection redisConnection, TWork work)
        {
            var redisWorkerNamingStrategy = new DefaultRedisWorkerNamingStrategy(typeof (TWork).Name);
            QueueWork(redisConnection, work, redisWorkerNamingStrategy);
        }
    }
}