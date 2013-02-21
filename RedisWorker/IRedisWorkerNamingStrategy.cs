namespace RedisWorker
{
    public interface IRedisWorkerNamingStrategy
    {
        string QueueName { get; }
        string CleanupLockName { get; }
        string WorkName { get; }
        string ProcessingName { get; }
        string AuditName { get; }
        string ErrorName { get; }
    }
}