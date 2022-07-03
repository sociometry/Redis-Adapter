using System.IO;
using StackExchange.Redis;

namespace Casbin.Adapter.Redis.Benchmark
{
    public static class TestHelper
    {
        public static string GetTestFilePath(string fileName)
        {
            return Path.Combine("examples", fileName);
        }
    }
}
