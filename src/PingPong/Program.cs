using System;
using System.Threading.Tasks;
using Apache.NMS.AMQP;

namespace PingPong
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var connectionFactory = new NmsConnectionFactory();
            connectionFactory.UserName = "artemis";
            connectionFactory.Password = "simetraehcapa";

            using (var ping = new Ping(connectionFactory))
            using (new Pong(connectionFactory))
            {
                var stats = await ping.Start(skipMessages: 100, numberOfMessages: 10000);
                Console.WriteLine(stats);
            }
        }
    }
}