using System;

namespace RedisWorker
{
    public interface IRedisWorker<out TWork>
    {
        void WaitForWork(Action<TWork> workHandler);
    }
}