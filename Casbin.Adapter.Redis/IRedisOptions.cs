namespace Casbin.Adapter.Redis
{
    public interface IRedisOptions
    {
        public string Address { get; set; }
        public string Password { get; set; }
        public string Key { get; set; }
    }
}