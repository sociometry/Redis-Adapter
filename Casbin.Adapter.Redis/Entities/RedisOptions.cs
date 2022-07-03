namespace Casbin.Adapter.Redis.Entities
{
    public class RedisOptions : IRedisOptions
    {
        public string Address { get; set; }
        public string Password { get; set; }
        public string Key { get; set; }
    }
}