using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BookSleeve;
using Newtonsoft.Json;

namespace RedisWorker
{
    public class RedisWorker<TWork> : IRedisWorker<TWork>
    {
        private readonly RedisConnection _blockingRedisConnection;
        private readonly RedisConnection _nonBlockingRedisConnection;
        private readonly IRedisWorkerOptions _redisWorkerOptions;

        public RedisWorker(
            Func<RedisConnection> redisConnection,
            IRedisWorkerOptions redisWorkerOptions)
        {
            _blockingRedisConnection = redisConnection.Invoke();
            _nonBlockingRedisConnection = redisConnection.Invoke();
            _redisWorkerOptions = redisWorkerOptions;
        }

        public RedisWorker(
            Func<RedisConnection> redisConnection)
            : this(redisConnection, new DefaultRedisWorkerOptions<TWork>())
        {
        }

        private void CompletedWork(string workId, RedisWork<TWork> redisWork)
        {
            using (var redisTransaction = _nonBlockingRedisConnection.CreateTransaction())
            {
                redisWork.WhenCompleted = DateTime.UtcNow;
                var serializedRedisWork = JsonConvert.SerializeObject(redisWork);

                redisTransaction.Lists.Remove(_redisWorkerOptions.RedisDatabase, _redisWorkerOptions.NamingStrategy.ProcessingName, workId);
                redisTransaction.Hashes.Remove(_redisWorkerOptions.RedisDatabase, _redisWorkerOptions.NamingStrategy.WorkName, workId);

                if (_redisWorkerOptions.Audit)
                {
                    redisTransaction.Hashes.Set(_redisWorkerOptions.RedisDatabase, _redisWorkerOptions.NamingStrategy.AuditName, workId,
                                                serializedRedisWork);
                }

                redisTransaction.Execute();
            }
        }

        private void ErroredWork(string workId, RedisWork<TWork> redisWork)
        {
            using (var redisTransaction = _nonBlockingRedisConnection.CreateTransaction())
            {
                redisTransaction.Lists.Remove(_redisWorkerOptions.RedisDatabase, _redisWorkerOptions.NamingStrategy.ProcessingName, workId);

                if (redisWork.RetryCount.Equals(_redisWorkerOptions.Retries))
                {
                    redisWork.WhenErrored = DateTime.UtcNow;
                    var serializedRedisWork = JsonConvert.SerializeObject(redisWork);

                    redisTransaction.Hashes.Remove(_redisWorkerOptions.RedisDatabase, _redisWorkerOptions.NamingStrategy.WorkName, workId);
                    redisTransaction.Hashes.Set(_redisWorkerOptions.RedisDatabase, _redisWorkerOptions.NamingStrategy.ErrorName, workId,
                                                serializedRedisWork);
                }
                else
                {
                    redisWork.RetryCount++;
                    var serializedRedisWork = JsonConvert.SerializeObject(redisWork);

                    redisTransaction.Lists.AddLast(_redisWorkerOptions.RedisDatabase, _redisWorkerOptions.NamingStrategy.QueueName, workId);
                    redisTransaction.Hashes.Set(_redisWorkerOptions.RedisDatabase, _redisWorkerOptions.NamingStrategy.WorkName, workId,
                                                serializedRedisWork);
                }

                redisTransaction.Execute();
            }
        }

        private void AcquireLock()
        {
            while (true)
            {
                var lockExpires = DateTime.UtcNow.Add(_redisWorkerOptions.OrphanedInProcessInterval);
                var lockExpiresUnixTimestamp = (lockExpires - new DateTime(1970, 1, 1, 0, 0, 0, 0, 0)).TotalSeconds + 1;
                var lockExpiresUnixTimestampString = lockExpiresUnixTimestamp.ToString(CultureInfo.CurrentCulture);
                var success =
                    _nonBlockingRedisConnection.Strings.SetIfNotExists(_redisWorkerOptions.RedisDatabase,
                                                                       _redisWorkerOptions.NamingStrategy.CleanupLockName,
                                                                       lockExpiresUnixTimestampString)
                                               .Result;

                if (success)
                {
                    Trace.WriteLine("No lock existed, success!");
                    return;
                }

                var existingLock =
                    _nonBlockingRedisConnection.Strings.GetString(_redisWorkerOptions.RedisDatabase, _redisWorkerOptions.NamingStrategy.CleanupLockName)
                                               .Result;
                double existingLockUnixTimestamp;
                if (!double.TryParse(existingLock, out existingLockUnixTimestamp))
                {
                    Trace.WriteLine(string.Format("Removing lock because it was unexpected data type, contained '{0}'", existingLock));
                    _nonBlockingRedisConnection.Keys.Remove(_redisWorkerOptions.RedisDatabase, _redisWorkerOptions.NamingStrategy.CleanupLockName);
                }

                if (lockExpiresUnixTimestamp > existingLockUnixTimestamp)
                {
                    Trace.WriteLine("Lock is out of date, updating with new value");
                    var getSetResult =
                        _nonBlockingRedisConnection.Strings.GetSet(_redisWorkerOptions.RedisDatabase,
                                                                   _redisWorkerOptions.NamingStrategy.CleanupLockName,
                                                                   lockExpiresUnixTimestampString)
                                                   .Result;

                    if (getSetResult.Equals(existingLock))
                    {
                        Trace.WriteLine("Acquired lock, success!");
                        return;
                    }
                }

                Trace.WriteLine(string.Format("Sleeping for {0} seconds", _redisWorkerOptions.OrphanedInProcessInterval.Seconds));
                Thread.Sleep(_redisWorkerOptions.OrphanedInProcessInterval);
            }
        }

        private void RequeueOrphanedInProcessWork()
        {
            while (true)
            {
                AcquireLock();

                var workIds =
                    _nonBlockingRedisConnection.Lists.RangeString(_redisWorkerOptions.RedisDatabase,
                                                                  _redisWorkerOptions.NamingStrategy.ProcessingName,
                                                                  0,
                                                                  int.MaxValue)
                                               .Result;
                foreach (var workId in workIds)
                {
                    var serializedRedisWork =
                        _nonBlockingRedisConnection.Hashes.GetString(_redisWorkerOptions.RedisDatabase,
                                                                     _redisWorkerOptions.NamingStrategy.WorkName,
                                                                     workId)
                                                   .Result;

                    if (serializedRedisWork == null)
                    {
                        continue;
                    }

                    var redisWork = JsonConvert.DeserializeObject<RedisWork<TWork>>(serializedRedisWork);
                            
                    if (redisWork.WhenQueued >= DateTime.UtcNow - _redisWorkerOptions.ProcessingGracePeriod)
                    {
                        continue;
                    }

                    using (var redisTransaction = _nonBlockingRedisConnection.CreateTransaction())
                    {
                        var id = workId;
                        Trace.WriteLine(string.Format("Requeueing work '{0}'", workId));
                        redisTransaction.Lists.Remove(_redisWorkerOptions.RedisDatabase, _redisWorkerOptions.NamingStrategy.ProcessingName, id);
                        redisTransaction.Lists.AddLast(_redisWorkerOptions.RedisDatabase, _redisWorkerOptions.NamingStrategy.QueueName, id);

                        redisTransaction.Execute();
                    }
                }

                Thread.Sleep(_redisWorkerOptions.OrphanedInProcessInterval);
            }
        }

        public void WaitForWork(Action<TWork> workHandler)
        {
            if (workHandler == null)
            {
                throw new ArgumentNullException("workHandler");
            }

            _nonBlockingRedisConnection.Open().Wait();

            Task.Factory.StartNew(RequeueOrphanedInProcessWork, TaskCreationOptions.LongRunning);

            var limitedConcurrencyLevelTaskScheduler = new LimitedConcurrencyLevelTaskScheduler(_redisWorkerOptions.MaxDegreeOfParallelism);
            var factory = new TaskFactory(limitedConcurrencyLevelTaskScheduler);

            while (true)
            {
                _blockingRedisConnection.Open().Wait();
                var workId = _blockingRedisConnection.Lists.BlockingRemoveLastAndAddFirstString(_redisWorkerOptions.RedisDatabase,
                                                                           _redisWorkerOptions.NamingStrategy.QueueName,
                                                                           _redisWorkerOptions.NamingStrategy
                                                                                              .ProcessingName, 0).Result;

                var serializedRedisWork =
                    _nonBlockingRedisConnection.Hashes.GetString(_redisWorkerOptions.RedisDatabase,
                                                                 _redisWorkerOptions.NamingStrategy.WorkName,
                                                                 workId)
                                               .Result;
                var redisWork = JsonConvert.DeserializeObject<RedisWork<TWork>>(serializedRedisWork);

                factory.StartNew(() =>
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
