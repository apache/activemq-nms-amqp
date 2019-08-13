using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS;
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Moq;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;
using Test.Amqp;


namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class MessageExpirationIntegrationTest
    {
        private static readonly string User = "USER";
        private static readonly string Password = "PASSWORD";
        private static readonly string Address = "amqp://127.0.0.1:5672";
        private static readonly IPEndPoint IPEndPoint = new IPEndPoint(IPAddress.Any, 5672);

        [Test, Timeout(4000)]
        public void TestIncomingExpiredMessageGetsFiltered()
        {
            const long ttl = 200;
            TimeSpan time = TimeSpan.FromMilliseconds(ttl + 1);
            using (TestAmqpPeer testPeer = new TestAmqpPeer(Address, User, Password))
            {
                DateTime createTime = DateTime.UtcNow - time;
                Amqp.Message expiredMsg = CreateMessageWithExpiration(ttl, createTime);
                string contents = (expiredMsg.BodySection as Amqp.Framing.AmqpValue)?.Value as string;
                Assert.NotNull(contents, "Failed to create expired message");
                testPeer.Open();
                testPeer.SendMessage("myQueue", expiredMsg);

                NmsConnection connection = (NmsConnection)EstablishConnection(testPeer);
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);
                // TODO change to verify frames sent from client to be a Modified OutCome
                IMessage message = consumer.Receive(time);
                Assert.IsNull(message, "A message should not have been received");

                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(4000)]
        public void TestIncomingExpiredMessageGetsConsumedWhenDisabled()
        {
            const long ttl = 200;
            TimeSpan time = TimeSpan.FromMilliseconds(ttl + 1);
            using (TestAmqpPeer testPeer = new TestAmqpPeer(Address, User, Password))
            {
                DateTime createTime = DateTime.UtcNow - time;
                Amqp.Message expiredMsg = CreateMessageWithExpiration(ttl, createTime);
                string contents = (expiredMsg.BodySection as Amqp.Framing.AmqpValue)?.Value as string;
                Assert.NotNull(contents, "Failed to create expired message");
                testPeer.Open();
                testPeer.SendMessage("myQueue", expiredMsg);

                NmsConnection connection = (NmsConnection)EstablishConnection(testPeer, "nms.localMessageExpiry=false");
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);
                IMessage message = consumer.Receive();
                // TODO change to verify frames sent from client to be an Accepted OutCome
                Assert.NotNull(message, "A message should have been received");
                Assert.NotNull(message as ITextMessage, "Received incorrect message body type {0}", message.GetType().Name);
                Assert.AreEqual(contents, (message as ITextMessage)?.Text, "Received message with unexpected body value");

                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(4000)]
        public void TestIncomingExpiredMessageGetsFilteredAsync()
        {
            const long ttl = 200;
            TimeSpan time = TimeSpan.FromMilliseconds(ttl + 1);
            using (TestAmqpPeer testPeer = new TestAmqpPeer(Address, User, Password))
            {
                DateTime createTime = DateTime.UtcNow - time;
                Amqp.Message expiredMsg = CreateMessageWithExpiration(ttl, createTime);
                string contents = (expiredMsg.BodySection as Amqp.Framing.AmqpValue)?.Value as string;
                Assert.NotNull(contents, "Failed to create expired message");
                testPeer.Open();
                testPeer.SendMessage("myQueue", expiredMsg);

                NmsConnection connection = (NmsConnection)EstablishConnection(testPeer);
                
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);
                TaskCompletionSource<IMessage> tcs = new TaskCompletionSource<IMessage>();
                consumer.Listener += m => { tcs.SetResult(m); };
                connection.Start();
                // TODO change to verify frames sent from client to be a Modified OutCome
                Assert.AreEqual(1, Task.WaitAny(tcs.Task, Task.Delay(Convert.ToInt32(ttl))), "Received message when message should not have been received");
                Assert.IsTrue(tcs.TrySetCanceled(), "Failed to cancel receive task");
                
                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(4000)]
        public void TestIncomingExpiredMessageGetsConsumedWhenDisabledAsync()
        {
            const long ttl = 200;
            TimeSpan time = TimeSpan.FromMilliseconds(ttl + 1);
            using (TestAmqpPeer testPeer = new TestAmqpPeer(Address, User, Password))
            {
                DateTime createTime = DateTime.UtcNow - time;
                Amqp.Message expiredMsg = CreateMessageWithExpiration(ttl, createTime);
                string contents = (expiredMsg.BodySection as Amqp.Framing.AmqpValue)?.Value as string;
                Assert.NotNull(contents, "Failed to create expired message");
                testPeer.Open();
                testPeer.SendMessage("myQueue", expiredMsg);

                NmsConnection connection = (NmsConnection)EstablishConnection(testPeer, "nms.localMessageExpiry=false");
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");
                IMessageConsumer consumer = session.CreateConsumer(queue);
                TaskCompletionSource<IMessage> tcs = new TaskCompletionSource<IMessage>();
                consumer.Listener += m => { tcs.SetResult(m); };
                connection.Start();
                // TODO change to verify frames sent from client to be an Accepted OutCome
                Assert.AreEqual(0, Task.WaitAny(tcs.Task, Task.Delay(Convert.ToInt32(ttl))), "Did not receive message when message should have been received");
                IMessage message = tcs.Task.Result;
                Assert.NotNull(message, "A message should have been received");
                Assert.NotNull(message as ITextMessage, "Received incorrect message body type {0}", message.GetType().Name);
                Assert.AreEqual(contents, (message as ITextMessage)?.Text, "Received message with unexpected body value");

                session.Close();
                connection.Close();
            }
        }

        private static Amqp.Message CreateMessageWithExpiration(long ttl, DateTime? createTime = null, string payload = null)
        {
            AmqpNmsTextMessageFacade msg = new AmqpNmsTextMessageFacade();
            msg.Initialize(null);
            msg.NMSTimestamp = createTime ?? DateTime.UtcNow;
            if (ttl > 0)
            {
                TimeSpan timeToLive = TimeSpan.FromMilliseconds(Convert.ToDouble(ttl));
                msg.NMSTimeToLive = timeToLive;
                msg.Expiration = msg.NMSTimestamp + timeToLive;
            }
            if (String.IsNullOrEmpty(payload))
            {
                payload = TestContext.CurrentContext.Test.FullName;
            }
            msg.Text = payload;
            msg.NMSMessageId = Guid.NewGuid().ToString();
            return msg.Message;
        }

        private static IConnection EstablishConnection(TestAmqpPeer peer, string queryParams = null)
        {
            string uri = String.IsNullOrEmpty(queryParams) ? peer.Address.OriginalString : $"{peer.Address.OriginalString}?{queryParams}";
            NmsConnectionFactory factory = new NmsConnectionFactory(uri);
            return factory.CreateConnection(User, Password);
        }
    }
}
