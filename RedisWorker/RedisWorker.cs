using System;
using System.Threading;
using ServiceStack.Redis;
using ServiceStack.Text;

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

    public class RedisWorker<TWork>
    {
        private readonly IRedisClientsManager _redisClientsManager; 
        private readonly IRedisWorkerNamingStrategy _redisWorkerNamingStrategy;

        public RedisWorker(
            IRedisClientsManager redisClientsManager,
            IRedisWorkerNamingStrategy redisWorkerNamingStrategy)
        {
            _redisClientsManager = redisClientsManager;
            _redisWorkerNamingStrategy = redisWorkerNamingStrategy;
        }

        public RedisWorker(
            IRedisClientsManager redisClientsManager)
            : this(redisClientsManager, new DefaultRedisWorkerNamingStrategy(typeof (TWork).Name))
        {
        }

        private IRedisClient GetRedisClient()
        {
            return _redisClientsManager.GetClient();
        }

        private void CompletedWork(string workId, RedisWork<TWork> redisWork)
        {
            using (var redisClient = GetRedisClient())            
            using (var redisTransaction = redisClient.CreateTransaction())
            {
                redisWork.WhenCompleted = DateTime.UtcNow;
                var serializedRedisWork = JsonSerializer.SerializeToString(redisWork);

                redisTransaction.QueueCommand(client => client.RemoveItemFromList(_redisWorkerNamingStrategy.ProcessingName, workId));
                redisTransaction.QueueCommand(client => client.RemoveEntryFromHash(_redisWorkerNamingStrategy.WorkName, workId));
                redisTransaction.QueueCommand(client => client.SetEntryInHash(_redisWorkerNamingStrategy.AuditName, workId, serializedRedisWork));

                redisTransaction.Commit();
            }
        }

        private void ErroredWork(string workId, RedisWork<TWork> redisWork)
        {
            const int maxRetryCount = 3;

            using (var redisClient = GetRedisClient())
            using (var redisTransaction = redisClient.CreateTransaction())
            {
                redisTransaction.QueueCommand(client => client.RemoveItemFromList(_redisWorkerNamingStrategy.ProcessingName, workId));

                if (redisWork.RetryCount++ > maxRetryCount)
                {
                    redisWork.WhenErrored = DateTime.UtcNow;
                    var serializedRedisWork = JsonSerializer.SerializeToString(redisWork);

                    redisTransaction.QueueCommand(client => client.RemoveEntryFromHash(_redisWorkerNamingStrategy.WorkName, workId));
                    redisTransaction.QueueCommand(client => client.SetEntryInHash(_redisWorkerNamingStrategy.ErrorName, workId, serializedRedisWork));
                }
                else
                {
                    var serializedRedisWork = JsonSerializer.SerializeToString(redisWork);

                    redisTransaction.QueueCommand(client => client.PushItemToList(_redisWorkerNamingStrategy.QueueName, workId));
                    redisTransaction.QueueCommand(client => client.SetEntryInHash(_redisWorkerNamingStrategy.WorkName, workId, serializedRedisWork));
                }

                redisTransaction.Commit();
            }
        }

        private void CleanupWork()
        {
            var cleanupDelay = new TimeSpan(0, 0, 10);

            while (true)
            {
                Console.WriteLine("Cleanup!");

                using (var redisClient = GetRedisClient())
                try
                {
                    using (redisClient.AcquireLock(_redisWorkerNamingStrategy.CleanupLockName, cleanupDelay))
                    {
                        var workIds = redisClient.GetAllItemsFromList(_redisWorkerNamingStrategy.ProcessingName);
                        foreach (var workId in workIds)
                        {
                            var serializedRedisWork = redisClient.GetValueFromHash(_redisWorkerNamingStrategy.WorkName, workId);
                            var redisWork = JsonSerializer.DeserializeFromString<RedisWork<TWork>>(serializedRedisWork);
                            if (redisWork.WhenQueued < DateTime.UtcNow - new TimeSpan(0, 5, 0))
                            {
                                Console.WriteLine("{0} Recovered orphaned work", workId);

                                using (var redisTransaction = redisClient.CreateTransaction())
                                {
                                    redisTransaction.QueueCommand(client => client.RemoveItemFromList(_redisWorkerNamingStrategy.ProcessingName, workId));
                                    redisTransaction.QueueCommand(client => client.PushItemToList(_redisWorkerNamingStrategy.QueueName, workId));

                                    redisTransaction.Commit();
                                }
                            }
                        }

                        Thread.Sleep(cleanupDelay);

                    }
                }
                catch (TimeoutException timeoutException)
                {
                    
                }
            }
        }

        public void WaitForWork(Action<TWork> workHandler)
        {
            if (workHandler == null)
            {
                throw new ArgumentNullException("workHandler");
            }

            var redisClient = GetRedisClient();

            ThreadPool.QueueUserWorkItem(delegate
                {
                    CleanupWork();
                });

            while (true)
            {
                var workId = redisClient.BlockingPopAndPushItemBetweenLists(_redisWorkerNamingStrategy.QueueName, _redisWorkerNamingStrategy.ProcessingName, null);
                var serializedRedisWork = redisClient.GetValueFromHash(_redisWorkerNamingStrategy.WorkName, workId);
                var redisWork = JsonSerializer.DeserializeFromString<RedisWork<TWork>>(serializedRedisWork);

                ThreadPool.QueueUserWorkItem(delegate
                {
                    try
                    {
                        workHandler.Invoke(redisWork.Work);
                        CompletedWork(workId, redisWork);
                    }
                    catch (Exception exception)
                    {
                        ErroredWork(workId, redisWork);
                    }
                });
            }
        }
    }
}
