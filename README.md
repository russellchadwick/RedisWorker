Purpose
===========

Provide worker functionality for Redis similar to publish/subscribe, when you want to schedule an asynchronous task to be executed once and only once.

Features
===========
 - Retries when handling work causes uncaught exceptions
 - Handles work concurrently using the ThreadPool
 - Saves completed work into an audit hash
 - Requeues work dropped by a crashed or disconnected worker

How to Use
===========

### Schedule work to be done using a simple as possible C# object that can be JSON serialized.
```
class WorkMessage
{
    public Guid Id { get; set; }
}

class WorkQueuer
{
    public static void Main()
    {
        var redisClientsManager = new PooledRedisClientManager("127.0.0.1");
        var redisClient = redisClientsManager.GetClient();
        redisClient.QueueWork(new WorkMessage
            {
                Id = Guid.NewGuid()
            });
    }
}
```

### Define how to handle scheduled work, ideally running multiple copies of worker to provide redundancy
```
class Worker
{
    public static void Main()
    {
        var redisClientsManager = new PooledRedisClientManager("127.0.0.1");
        var redisWorker = new RedisWorker<WorkMessage>(redisClientsManager);
        redisWorker.MaxDegreeOfParallelism = 10;
        redisWorker.WaitForWork(workMessage =>
            {
                Console.WriteLine("Received work {0}", workMessage.Id);
            });
    }
}
```
