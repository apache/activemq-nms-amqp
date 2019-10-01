using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Apache.NMS;

namespace PingPong
{
    public class Ping: IDisposable
    {
        private readonly IConnection connection;
        private readonly ISession session;
        private readonly IMessageProducer messageProducer;
        private readonly IMessageConsumer messageConsumer;
        private readonly Stopwatch stopwatch;
        private readonly ITextMessage pingMessage;
        private TaskCompletionSource<Stats> tsc;
        private int numberOfMessages;
        private int skipMessages;
        private int counter;

        public Ping(IConnectionFactory connectionFactory)
        {
            this.connection = connectionFactory.CreateConnection();
            this.session = this.connection.CreateSession();
            this.messageProducer = session.CreateProducer(session.GetTopic("ping"));
            this.messageConsumer = session.CreateConsumer(session.GetTopic("pong"));
            this.messageConsumer.Listener += OnMessage;
            this.stopwatch = new Stopwatch();
            this.pingMessage = session.CreateTextMessage("Ping");
            this.connection.Start();
        }

        private void OnMessage(IMessage message)
        {
            if (skipMessages > 0)
                skipMessages--;
            else
                counter++;

            if (counter == numberOfMessages)
            {
                stopwatch.Stop();
                this.tsc.SetResult(new Stats { MessagesCount = counter, Elapsed = stopwatch.Elapsed });                
            }
            else
            {
                messageProducer.Send(pingMessage);
            }
        }

        public Task<Stats> Start(int numberOfMessages, int skipMessages)
        {
            this.numberOfMessages = numberOfMessages;
            this.skipMessages = skipMessages;
            this.tsc = new TaskCompletionSource<Stats>();
            stopwatch.Start();
            messageProducer.Send(pingMessage);
            return this.tsc.Task;
        }

        public void Dispose()
        {
            connection?.Dispose();
        }
    }
}