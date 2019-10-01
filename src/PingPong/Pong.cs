using System;
using Apache.NMS;

namespace PingPong
{
    public class Pong : IDisposable
    {
        private readonly IConnection connection;
        private readonly ISession session;
        private readonly IMessageProducer messageProducer;
        private readonly IMessageConsumer messageConsumer;
        private readonly ITextMessage pongMessage;

        public Pong(IConnectionFactory connectionFactory)
        {
            this.connection = connectionFactory.CreateConnection();
            this.session = this.connection.CreateSession();
            this.messageProducer = session.CreateProducer(session.GetTopic("pong"));
            this.messageConsumer = session.CreateConsumer(session.GetTopic("ping"));
            this.messageConsumer.Listener += OnMessage;
            this.pongMessage = session.CreateTextMessage("Pong");
            this.connection.Start();
        }

        private void OnMessage(IMessage message)
        {
            this.messageProducer.Send(pongMessage);
        }

        public void Dispose()
        {
            connection?.Dispose();
        }
    }
}