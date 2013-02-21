using System;
using ServiceStack.Redis;
using ServiceStack.Text;

namespace RedisWorker
{
    public static class RedisWorkerQueuer
    {
        public static void QueueWork<TWork>(this IRedisClient redisClient, TWork work, IRedisWorkerNamingStrategy redisWorkerNamingStrategy)
        {
            var workId = Guid.NewGuid().ToString();

            using (var redisTransaction = redisClient.CreateTransaction())
            {
                var redisWork = new RedisWork<TWork>
                    {
                        WhenQueued = DateTime.UtcNow,
                        RetryCount = 0,
                        Work = work
                    };

                var serializedRedisWork = JsonSerializer.SerializeToString(redisWork);

                redisTransaction.QueueCommand(client => client.PushItemToList(redisWorkerNamingStrategy.QueueName, workId));
                redisTransaction.QueueCommand(client => client.SetEntryInHash(redisWorkerNamingStrategy.WorkName, workId, serializedRedisWork));

                redisTransaction.Commit();
            }
        }

        public static void QueueWork<TWork>(this IRedisClient redisClient, TWork work)
        {
            var redisWorkerNamingStrategy = new DefaultRedisWorkerNamingStrategy(typeof (TWork).Name);
            QueueWork(redisClient, work, redisWorkerNamingStrategy);
        }
    }
}