/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS;
using NUnit.Framework;

namespace NMS.AMQP.Test
{
    [TestFixture]
    public class NmsMessageConsumerTest : AmqpTestSupport
    {
        [Test, Timeout(60_000)]
        public void TestSelectors()
        {
            PurgeQueue(TimeSpan.FromMilliseconds(500));

            Connection = CreateAmqpConnection();
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IQueue queue = session.GetQueue(TestName);
            IMessageProducer producer = session.CreateProducer(queue);

            ITextMessage message = session.CreateTextMessage("Hello");
            producer.Send(message, MsgDeliveryMode.Persistent, MsgPriority.Normal, TimeSpan.Zero);

            string text = "Hello + 9";
            message = session.CreateTextMessage(text);
            producer.Send(message, MsgDeliveryMode.Persistent, MsgPriority.Highest, TimeSpan.Zero);

            producer.Close();

            IMessageConsumer messageConsumer = session.CreateConsumer(queue, "JMSPriority > 8");
            IMessage msg = messageConsumer.Receive(TimeSpan.FromSeconds(5));
            Assert.NotNull(msg, "No message was received");
            Assert.IsInstanceOf<ITextMessage>(msg);
            Assert.AreEqual(text, ((ITextMessage) msg).Text);
            Assert.IsNull(messageConsumer.Receive(TimeSpan.FromSeconds(1)));
            
            messageConsumer.Close();
            
        }
        
        [Test, Timeout(60_000)]
        public void TestConsumerCredit()
        {
            PurgeQueue(TimeSpan.FromMilliseconds(500));

            Connection = CreateAmqpConnection(options: "nms.prefetchPolicy.all=3");
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.ClientAcknowledge);
            IQueue queue = session.GetQueue(TestName);
            IMessageConsumer messageConsumer = session.CreateConsumer(queue);
            ConcurrentBag<IMessage> messages = new ConcurrentBag<IMessage>();
            CountdownEvent countdownReceived = new CountdownEvent(4);
            messageConsumer.Listener += message =>
            {
                messages.Add(message);
                try
                {
                    countdownReceived.Signal();
                }
                catch (Exception)
                {
                    // if it gets below zero, we dont care
                }
            };
            
            IMessageProducer producer = session.CreateProducer(queue);
            Enumerable.Range(0, 100).ToList().ForEach(nr => producer.Send(session.CreateTextMessage("hello")));
            
            // Wait for at least four messages are read, which should never happen
            Assert.IsFalse(countdownReceived.Wait(500));
            Assert.AreEqual(3, messages.Count);
            
            // Once we ack messages we should start receiving another 3
            messages.ToList().ForEach(m => m.Acknowledge());
            // We just wait to see if 4th message arrived
            Assert.IsTrue(countdownReceived.Wait(500));
        }

        
        [Test, Timeout(60_000)]
        public void TestSelectorsWithJMSType()
        {
            PurgeQueue(TimeSpan.FromMilliseconds(500));

            Connection = CreateAmqpConnection();
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IQueue queue = session.GetQueue(TestName);
            IMessageProducer producer = session.CreateProducer(queue);

            ITextMessage message1 = session.CreateTextMessage("text");
            producer.Send(message1, MsgDeliveryMode.Persistent, MsgPriority.Normal, TimeSpan.Zero);

            string type = "myJMSType";
            string text = "text" + type;
            ITextMessage message2 = session.CreateTextMessage(text);
            message2.NMSType = type;
            producer.Send(message2, MsgDeliveryMode.Persistent, MsgPriority.Highest, TimeSpan.Zero);

            producer.Close();

            IMessageConsumer messageConsumer = session.CreateConsumer(queue, $"JMSType = '{type}'");
            IMessage msg = messageConsumer.Receive(TimeSpan.FromSeconds(5));
            Assert.NotNull(msg, "No message was received");
            Assert.IsInstanceOf<ITextMessage>(msg);
            Assert.AreEqual(text, ((ITextMessage) msg).Text);
            Assert.IsNull(messageConsumer.Receive(TimeSpan.FromSeconds(1)));
        }


        [Test, Timeout(60_000)]
        public void TestDurableSubscription()
        {
            Connection = CreateAmqpConnection();
            Connection.Start();

            int counter = 0;


            using (ISession sessionProducer = Connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
            {
                string subscriptionName = "mySubscriptionName";
                ITopic topicProducer = sessionProducer.GetTopic(TestName);
                using (IMessageProducer producer = sessionProducer.CreateProducer(topicProducer))
                {
                    // First durable consumer, reads message but does not unsubscribe
                    using (var connectionSubscriber = CreateAmqpConnectionStarted("CLIENT1"))
                    using (ISession session = connectionSubscriber.CreateSession(AcknowledgementMode.AutoAcknowledge))
                    {
                        using (ITopic topic = session.GetTopic(TestName))
                        using (IMessageConsumer messageConsumer = session.CreateDurableConsumer(topic, subscriptionName, null))
                        {
                            // Purge topic
                            PurgeConsumer(messageConsumer, TimeSpan.FromSeconds(0.5));

                            ITextMessage producerMessage = sessionProducer.CreateTextMessage("text" + (counter++));
                            producer.Send(producerMessage, MsgDeliveryMode.Persistent, MsgPriority.Normal, TimeSpan.Zero);

                            var message = messageConsumer.Receive();
                            Assert.AreEqual("text0", message.Body<string>());
                        }
                    }

                    // Write some more messages while subscription is closed
                    for (int t = 0; t < 3; t++)
                    {
                        ITextMessage producerMessage = sessionProducer.CreateTextMessage("text" + (counter++));
                        producer.Send(producerMessage, MsgDeliveryMode.Persistent, MsgPriority.Normal, TimeSpan.Zero);
                    }

                    // Second durable consumer, reads message that were send during no-subscription and unsubscribe
                    using (var connectionSubscriber = CreateAmqpConnectionStarted("CLIENT1"))
                    using (ISession session = connectionSubscriber.CreateSession(AcknowledgementMode.AutoAcknowledge))
                    {
                        using (ITopic topic = session.GetTopic(TestName))
                        using (IMessageConsumer messageConsumer = session.CreateDurableConsumer(topic, subscriptionName, null))
                        {
                            for (int t = 1; t <= 3; t++)
                            {
                                var message = messageConsumer.Receive();
                                Assert.AreEqual("text" + t, message.Body<string>());
                            }

                            // Assert topic is empty after those msgs
                            var msgAtTheEnd = messageConsumer.Receive(TimeSpan.FromSeconds(1));
                            Assert.IsNull(msgAtTheEnd);

                            Assert.Throws<IllegalStateException>(() => session.Unsubscribe(subscriptionName)); // Error unsubscribing while consumer is on
                        }

                        session.Unsubscribe(subscriptionName);
                    }


                    // Send some messages again to verify we will not get them when create durable subscription
                    for (int t = 0; t < 3; t++)
                    {
                        ITextMessage producerMessage = sessionProducer.CreateTextMessage("text" + (counter++));
                        producer.Send(producerMessage, MsgDeliveryMode.Persistent, MsgPriority.Normal, TimeSpan.Zero);
                    }

                    // Third durable subscriber, expect NOT to read messages during no-subscription period
                    using (var connectionSubscriber = CreateAmqpConnectionStarted("CLIENT1"))
                    using (ISession session = connectionSubscriber.CreateSession(AcknowledgementMode.AutoAcknowledge))
                    {
                        using (ITopic topic = session.GetTopic(TestName))
                        using (IMessageConsumer messageConsumer = session.CreateDurableConsumer(topic, subscriptionName, null))
                        {
                            // Assert topic is empty 
                            var msgAtTheEnd = messageConsumer.Receive(TimeSpan.FromSeconds(1));
                            Assert.IsNull(msgAtTheEnd);
                        }

                        // And unsubscribe again
                        session.Unsubscribe(subscriptionName);
                    }
                }
            }
        }


        [Test, Timeout(60_000)]
        public void TestSharedSubscription()
        {
            IMessageConsumer GetConsumer(string subName, String clientId)
            {
                var connection = CreateAmqpConnectionStarted(clientId);
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                var topic = session.GetTopic(TestName);
                var messageConsumer = session.CreateSharedConsumer(topic, subName);
                return messageConsumer;
            }

            Connection = CreateAmqpConnection();
            Connection.Start();

            string subscriptionName = "mySubscriptionName";


            var receivedMessages = new List<int>();

            var messageConsumer1 = GetConsumer(subscriptionName, null);
            var messageConsumer2 = GetConsumer(subscriptionName, null);
            messageConsumer1.Listener += (msg) =>
            {
                receivedMessages.Add(1);
                msg.Acknowledge();
            };
            messageConsumer2.Listener += (msg) =>
            {
                receivedMessages.Add(2);
                msg.Acknowledge();
            };

            // Now send some messages
            using (ISession sessionProducer = Connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
            {
                ITopic topicProducer = sessionProducer.GetTopic(TestName);
                using (IMessageProducer producer = sessionProducer.CreateProducer(topicProducer))
                {
                    for (int t = 0; t < 10; t++)
                    {
                        ITextMessage producerMessage = sessionProducer.CreateTextMessage("text" + t);
                        producer.Send(producerMessage, MsgDeliveryMode.Persistent, MsgPriority.Normal, TimeSpan.Zero);
                    }
                }
            }

            // Give it some time to process
            Thread.Sleep(TimeSpan.FromSeconds(2));

            // Assert message was routed to multiple consumers
            Assert.AreEqual(2, receivedMessages.Distinct().Count());
            Assert.AreEqual(10, receivedMessages.Count);
        }

        [Test, Timeout(60_000)]
        public void TestSharedDurableSubscription()
        {
            (IMessageConsumer, ISession, IConnection) GetConsumer(string subName, String clientId)
            {
                var connection = CreateAmqpConnection(clientId);
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                var topic = session.GetTopic(TestName);
                var messageConsumer = session.CreateSharedDurableConsumer(topic, subName);
                return (messageConsumer, session, connection);
            }

            Connection = CreateAmqpConnection();
            Connection.Start();

            string subscriptionName = "mySubscriptionName";
            int messageSendCount = 1099;

            var receivedMessages = new ConcurrentBag<int>();


            IConnection connectionConsumer1, connectionConsumer2;
            IMessageConsumer messageConsumer1, messageConsumer2;

            (messageConsumer1, _, connectionConsumer1) = GetConsumer(subscriptionName, null);
            (messageConsumer2, _, connectionConsumer2) = GetConsumer(subscriptionName, null);
            connectionConsumer1.Start();
            connectionConsumer2.Start();

            messageConsumer1.Close();
            messageConsumer2.Close();
            connectionConsumer1.Close();
            connectionConsumer2.Close();

            // Now send some messages
            using (ISession sessionProducer = Connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
            {
                ITopic topicProducer = sessionProducer.GetTopic(TestName);
                using (IMessageProducer producer = sessionProducer.CreateProducer(topicProducer))
                {
                    for (int t = 0; t < messageSendCount; t++)
                    {
                        ITextMessage producerMessage = sessionProducer.CreateTextMessage("text" + t);
                        producer.Send(producerMessage, MsgDeliveryMode.Persistent, MsgPriority.Normal, TimeSpan.Zero);
                    }
                }

                // Create consumers again and expect messages to be delivered to them
                ISession sessionConsumer1, sessionConsumer2;
                (messageConsumer1, sessionConsumer1, connectionConsumer1) = GetConsumer(subscriptionName, null);
                (messageConsumer2, sessionConsumer2, connectionConsumer2) = GetConsumer(subscriptionName, null);
                messageConsumer1.Listener += (msg) =>
                {
                    receivedMessages.Add(1);
                    msg.Acknowledge();
                };
                messageConsumer2.Listener += (msg) =>
                {
                    receivedMessages.Add(2);
                    msg.Acknowledge();
                };
                Task.Run(() => connectionConsumer1.Start()); // parallel to give both consumers chance to start at the same time
                Task.Run(() => connectionConsumer2.Start());


                // Give it some time to process
                Thread.Sleep(TimeSpan.FromSeconds(5));

                // Assert message was routed to multiple consumers
                Assert.AreEqual(2, receivedMessages.Distinct().Count());
                Assert.AreEqual(messageSendCount, receivedMessages.Count);

                messageConsumer1.Close();
                messageConsumer2.Close();
                
                sessionConsumer1.Unsubscribe(subscriptionName);
            }
        }


        [Test, Timeout(60_000)]
        public void TestSelectNoLocal()
        {
            PurgeTopic(TimeSpan.FromMilliseconds(500));

            Connection = CreateAmqpConnection();
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            ITopic topic = session.GetTopic(TestName);
            IMessageProducer producer = session.CreateProducer(topic);
            ITextMessage message = session.CreateTextMessage("text");
            producer.Send(message, MsgDeliveryMode.Persistent, MsgPriority.Normal, TimeSpan.Zero);
            IMessageConsumer messageConsumer = session.CreateConsumer(topic, null, noLocal: true);
            Assert.IsNull(messageConsumer.Receive(TimeSpan.FromMilliseconds(500)));
        }
    }
}