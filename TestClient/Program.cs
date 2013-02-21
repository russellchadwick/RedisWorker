using System;
using System.Threading;
using RedisWorker;
using ServiceStack.Redis;

namespace TestClient
{
    class PerformPretendWorkMessage
    {
        public Guid Id { get; set; }
    }

    class Program
    {
        private static readonly Func<IRedisClient> RedisClientProvider = () => new RedisClient("192.168.94.178");

        static void StartSomeWork(int count = 10)
        {
            var redisClient = RedisClientProvider.Invoke();

            for (var x = 0; x < count; x++)
            {
                var guid = Guid.NewGuid();
                redisClient.QueueWork(new PerformPretendWorkMessage
                    {
                        Id = guid
                    });
                Console.WriteLine("{0} Wrote work", guid);
            }
        }

        static void Main()
        {
            ThreadPool.QueueUserWorkItem(delegate
                {
                    StartSomeWork();
                });

            ThreadPool.QueueUserWorkItem(delegate
                {
                    var redisWorker = new RedisWorker<PerformPretendWorkMessage>(RedisClientProvider);
                    redisWorker.WaitForWork(performPretendWorkMessage =>
                        {
                            Console.WriteLine("{0} Received work", performPretendWorkMessage.Id);
                            var random = new Random();
                            var randomWait = random.Next(10);
                            Console.WriteLine("{0} Pretending it takes {1} seconds to complete",
                                              performPretendWorkMessage.Id, randomWait);
                            var chaosMonkey = random.Next(5).Equals(0);
                            if (chaosMonkey)
                            {
                                Console.WriteLine("{0} Going to throw exception on worker", performPretendWorkMessage.Id);
                            }
                            Thread.Sleep(randomWait*1000);
                            if (chaosMonkey)
                            {
                                Console.WriteLine("{0} Exception", performPretendWorkMessage.Id);
                                throw new Exception("Chaos monkey!");
                            }
                            Console.WriteLine("{0} Done with work ", performPretendWorkMessage.Id);
                        });
                });

            while (Console.ReadLine() != "Quit")
            {
                ThreadPool.QueueUserWorkItem(delegate
                    {
                        StartSomeWork();
                    });
            }
        }
    }
}
