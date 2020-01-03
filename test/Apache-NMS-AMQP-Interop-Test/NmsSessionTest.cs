using System.Threading.Tasks;
using Apache.NMS;
using NUnit.Framework;

namespace NMS.AMQP.Test
{
    public class NmsSessionTest : AmqpTestSupport
    {
        [Test, Timeout(10_000)]
        public void TestCreateMultipleSessionsFromDifferentThreadsWhenConnectionNotStarted()
        {
            Connection = CreateAmqpConnection();
            Assert.NotNull(Connection);

            Parallel.For(0, 10, i =>
            {
                ISession session = Connection.CreateSession();
                Assert.NotNull(session);
            });
        }
    }
}