namespace RedisWorker
{
    public class DefaultRedisWorkerNamingStrategy : IRedisWorkerNamingStrategy
    {
        private readonly string _prefix;

        public DefaultRedisWorkerNamingStrategy(string baseName)
        {
            _prefix = "RedisWorker:" + baseName;
        }

        public string QueueName
        {
            get { return _prefix; }
        }

        public string CleanupLockName
        {
            get { return _prefix + ":Lock"; }
        }

        public string WorkName
        {
            get { return _prefix + ":Work"; }
        }

        public string AuditName
        {
            get { return _prefix + ":Audit"; }
        }

        public string ProcessingName
        {
            get { return _prefix + ":Processing"; }
        }

        public string ErrorName
        {
            get { return _prefix + ":Error"; }
        }
    }
}