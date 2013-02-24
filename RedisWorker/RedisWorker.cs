using System;
using System.Threading;
using System.Threading.Tasks;
using ServiceStack.Redis;
using ServiceStack.Text;

namespace RedisWorker
{
    public class RedisWorker<TWork> : IRedisWorker<TWork>
    {
        private readonly IRedisClientsManager _redisClientsManager;
        private readonly IRedisWorkerOptions _redisWorkerOptions;

        public RedisWorker(
            IRedisClientsManager redisClientsManager,
            IRedisWorkerOptions redisWorkerOptions)
        {
            _redisClientsManager = redisClientsManager;
            _redisWorkerOptions = redisWorkerOptions;
        }

        public RedisWorker(
            IRedisClientsManager redisClientsManager)
            : this(redisClientsManager, new DefaultRedisWorkerOptions<TWork>())
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

                redisTransaction.QueueCommand(client => client.RemoveItemFromList(_redisWorkerOptions.NamingStrategy.ProcessingName, workId));
                redisTransaction.QueueCommand(client => client.RemoveEntryFromHash(_redisWorkerOptions.NamingStrategy.WorkName, workId));

                if (_redisWorkerOptions.Audit)
                {
                    redisTransaction.QueueCommand(client => client.SetEntryInHash(_redisWorkerOptions.NamingStrategy.AuditName, workId, serializedRedisWork));
                }

                redisTransaction.Commit();
            }
        }

        private void ErroredWork(string workId, RedisWork<TWork> redisWork)
        {
            using (var redisClient = GetRedisClient())
            using (var redisTransaction = redisClient.CreateTransaction())
            {
                redisTransaction.QueueCommand(client => client.RemoveItemFromList(_redisWorkerOptions.NamingStrategy.ProcessingName, workId));

                if (redisWork.RetryCount.Equals(_redisWorkerOptions.Retries))
                {
                    redisWork.WhenErrored = DateTime.UtcNow;
                    var serializedRedisWork = JsonSerializer.SerializeToString(redisWork);

                    redisTransaction.QueueCommand(client => client.RemoveEntryFromHash(_redisWorkerOptions.NamingStrategy.WorkName, workId));
                    redisTransaction.QueueCommand(client => client.SetEntryInHash(_redisWorkerOptions.NamingStrategy.ErrorName, workId, serializedRedisWork));
                }
                else
                {
                    redisWork.RetryCount++;
                    var serializedRedisWork = JsonSerializer.SerializeToString(redisWork);

                    redisTransaction.QueueCommand(client => client.PushItemToList(_redisWorkerOptions.NamingStrategy.QueueName, workId));
                    redisTransaction.QueueCommand(client => client.SetEntryInHash(_redisWorkerOptions.NamingStrategy.WorkName, workId, serializedRedisWork));
                }

                redisTransaction.Commit();
            }
        }

        private void RequeueOrphanedInProcessWork()
        {
            while (true)
            {
                using (var redisClient = GetRedisClient())
                try
                {
                    using (redisClient.AcquireLock(_redisWorkerOptions.NamingStrategy.CleanupLockName, _redisWorkerOptions.OrphanedInProcessInterval))
                    {
                        var workIds = redisClient.GetAllItemsFromList(_redisWorkerOptions.NamingStrategy.ProcessingName);
                        foreach (var workId in workIds)
                        {
                            var serializedRedisWork = redisClient.GetValueFromHash(_redisWorkerOptions.NamingStrategy.WorkName, workId);
                            var redisWork = JsonSerializer.DeserializeFromString<RedisWork<TWork>>(serializedRedisWork);
                            
                            if (redisWork.WhenQueued >= DateTime.UtcNow - _redisWorkerOptions.ProcessingGracePeriod)
                                continue;

                            using (var redisTransaction = redisClient.CreateTransaction())
                            {
                                redisTransaction.QueueCommand(client => client.RemoveItemFromList(_redisWorkerOptions.NamingStrategy.ProcessingName, workId));
                                redisTransaction.QueueCommand(client => client.PushItemToList(_redisWorkerOptions.NamingStrategy.QueueName, workId));

                                redisTransaction.Commit();
                            }
                        }

                        Thread.Sleep(_redisWorkerOptions.OrphanedInProcessInterval);
                    }
                }
                catch (TimeoutException)
                {
                    // Expected to time out, it means another worker is holding the lock and performing the work
                }
            }
        }

        public void WaitForWork(Action<TWork> workHandler)
        {
            if (workHandler == null)
            {
                throw new ArgumentNullException("workHandler");
            }

            Task.Factory.StartNew(RequeueOrphanedInProcessWork, TaskCreationOptions.LongRunning);

            var redisClient = GetRedisClient();
            while (true)
            {
                var workId = redisClient.BlockingPopAndPushItemBetweenLists(_redisWorkerOptions.NamingStrategy.QueueName, _redisWorkerOptions.NamingStrategy.ProcessingName, null);
                var serializedRedisWork = redisClient.GetValueFromHash(_redisWorkerOptions.NamingStrategy.WorkName, workId);
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
