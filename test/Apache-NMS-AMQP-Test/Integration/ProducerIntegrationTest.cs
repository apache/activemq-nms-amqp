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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Apache.NMS;
using Apache.NMS.AMQP;
using Moq;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class ProducerIntegrationTest
    {
        private static readonly string User = "USER";
        private static readonly string Password = "PASSWORD";
        private static readonly string Address = "amqp://127.0.0.1:5672";

        [Test, Timeout(2000)]
        public void TestCloseSender()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor();
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                Assert.IsNotNull(testLinkProcessor.Producer);

                producer.Close();

                Assert.That(() => testLinkProcessor.Producer, Is.Null.After(500));

                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestSentTextMessageCanBeModified()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor();
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                string text = "myMessage";
                ITextMessage textMessage = session.CreateTextMessage(text);
                producer.Send(textMessage);

                Assert.AreEqual(text, textMessage.Text);
                textMessage.Text = text + text;
                Assert.AreEqual(text + text, textMessage.Text);

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestDefaultDeliveryModeProducesDurableMessages()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor();
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                string text = "myMessage";
                ITextMessage textMessage = session.CreateTextMessage(text);
                producer.Send(textMessage);

                Assert.AreEqual(MsgDeliveryMode.Persistent, textMessage.NMSDeliveryMode);

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestProducerOverridesMessageDeliveryMode()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor();
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                string text = "myMessage";
                ITextMessage textMessage = session.CreateTextMessage(text);
                textMessage.NMSDeliveryMode = MsgDeliveryMode.NonPersistent;

                producer.Send(textMessage);

                Assert.AreEqual(MsgDeliveryMode.Persistent, textMessage.NMSDeliveryMode);

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestSendingMessageNonPersistentProducerSetDurableFalse()
        {
            DoSendingMessageNonPersistentTestImpl(false, true, true);
        }

        [Test, Ignore("TestAmqpPeer doesn't support anonymous producers")]
        public void TestSendingMessageNonPersistentProducerSetDurableFalseAnonymousProducer()
        {
            DoSendingMessageNonPersistentTestImpl(true, true, true);
        }

        [Test]
        public void TestSendingMessageNonPersistentSendSetDurableFalse()
        {
            DoSendingMessageNonPersistentTestImpl(false, true, false);
        }

        [Test, Ignore("TestAmqpPeer doesn't support anonymous producers")]
        public void TestSendingMessageNonPersistentSendSetDurableFalseAnonymousProducer()
        {
            DoSendingMessageNonPersistentTestImpl(true, true, false);
        }

        [Test]
        public void TestSendingMessageNonPersistentProducerOmitsHeader()
        {
            DoSendingMessageNonPersistentTestImpl(false, false, true);
        }

        [Test, Ignore("TestAmqpPeer doesn't support anonymous producers")]
        public void TestSendingMessageNonPersistentProducerOmitsHeaderAnonymousProducer()
        {
            DoSendingMessageNonPersistentTestImpl(true, false, true);
        }

        [Test]
        public void TestSendingMessageNonPersistentSendOmitsHeader()
        {
            DoSendingMessageNonPersistentTestImpl(false, false, false);
        }

        [Test, Ignore("TestAmqpPeer doesn't support anonymous producers")]
        public void TestSendingMessageNonPersistentSendOmitsHeaderAnonymousProducer()
        {
            DoSendingMessageNonPersistentTestImpl(true, false, false);
        }

        private void DoSendingMessageNonPersistentTestImpl(bool anonymousProducer, bool setPriority, bool setOnProducer)
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor();
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue queue = session.GetQueue("myQueue");

                IMessageProducer producer = null;
                if (anonymousProducer)
                {
                    producer = session.CreateProducer();
                }
                else
                {
                    producer = session.CreateProducer(queue);
                }

                string text = "myMessage";
                ITextMessage message = session.CreateTextMessage(text);

                if (setPriority)
                {
                    message.NMSPriority = MsgPriority.High;
                }

                if (setOnProducer)
                {
                    producer.DeliveryMode = MsgDeliveryMode.NonPersistent;

                    if (setPriority)
                    {
                        producer.Priority = MsgPriority.High;
                    }

                    if (anonymousProducer)
                        producer.Send(queue, message);
                    else
                        producer.Send(message);
                }
                else
                {
                    if (anonymousProducer)
                    {
                        producer.Send(queue, message, MsgDeliveryMode.NonPersistent, setPriority ? MsgPriority.High : NMSConstants.defaultPriority, NMSConstants.defaultTimeToLive);
                    }
                    else
                    {
                        producer.Send(message, MsgDeliveryMode.NonPersistent, setPriority ? MsgPriority.High : NMSConstants.defaultPriority, NMSConstants.defaultTimeToLive);
                    }
                }

                Assert.AreEqual(MsgDeliveryMode.NonPersistent, message.NMSDeliveryMode);

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestSendingMessageSetsNMSDestination()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                List<Amqp.Message> receivedMessages = new List<Amqp.Message>();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor(receivedMessages);
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                string text = "myMessage";
                ITextMessage message = session.CreateTextMessage(text);

                Assert.IsNull(message.NMSDestination);

                producer.Send(message);

                Assert.AreEqual(destination, message.NMSDestination);
                Assert.IsTrue(receivedMessages.Any());
                Assert.That(receivedMessages.First().Properties.To, Is.EqualTo(destination.QueueName));

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestSendingMessageSetsNMSTimestamp()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                List<Amqp.Message> receivedMessages = new List<Amqp.Message>();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor(receivedMessages);
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                DateTime timeStamp = DateTime.UtcNow;

                string text = "myMessage";
                ITextMessage message = session.CreateTextMessage(text);

                producer.Send(message);

                Assert.IsTrue(receivedMessages.Any());
                Assert.That(receivedMessages.First().Properties.CreationTime, Is.EqualTo(timeStamp).Within(TimeSpan.FromMilliseconds(100)));

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestSendingMessageSetsNMSExpirationRelatedAbsoluteExpiryAndTtlFields()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                List<Amqp.Message> receivedMessages = new List<Amqp.Message>();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor(receivedMessages);
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                TimeSpan ttl = TimeSpan.FromMilliseconds(100_000);
                DateTime expiration = DateTime.UtcNow + ttl;

                string text = "myMessage";
                ITextMessage message = session.CreateTextMessage(text);

                producer.Send(message, NMSConstants.defaultDeliveryMode, NMSConstants.defaultPriority, ttl);

                Assert.IsTrue(receivedMessages.Any());
                Assert.That(receivedMessages.First().Properties.AbsoluteExpiryTime, Is.EqualTo(expiration).Within(TimeSpan.FromMilliseconds(100)));

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestNonDefaultPriorityProducesMessagesWithPriorityFieldAndSetsNMSPriority()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                List<Amqp.Message> receivedMessages = new List<Amqp.Message>();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor(receivedMessages);
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                string text = "myMessage";
                ITextMessage message = session.CreateTextMessage(text);


                Assert.AreEqual(MsgPriority.BelowNormal, message.NMSPriority);
                producer.Send(message, MsgDeliveryMode.Persistent, MsgPriority.Highest, NMSConstants.defaultTimeToLive);

                Assert.AreEqual(MsgPriority.Highest, message.NMSPriority);
                Assert.IsTrue(receivedMessages.Any());
                Assert.AreEqual(9, receivedMessages.First().Header.Priority);

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestSendingMessageSetsNMSMessageId()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                List<Amqp.Message> receivedMessages = new List<Amqp.Message>();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor(receivedMessages);
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                string text = "myMessage";
                ITextMessage message = session.CreateTextMessage(text);

                Assert.IsNull(message.NMSMessageId);

                producer.Send(message);

                Assert.IsNotEmpty(message.NMSMessageId);
                Assert.IsTrue(message.NMSMessageId.StartsWith("ID:"));

                Assert.IsTrue(receivedMessages.Any());
                Assert.AreEqual(message.NMSMessageId, receivedMessages.First().Properties.MessageId);

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestSendingMessageWithDisableMessageIdHint()
        {
            DoSendingMessageWithDisableMessageIdHintTestImpl(false);
        }

        [Test]
        public void TestSendingMessageWithDisableMessageIdHintAndExistingMessageId()
        {
            DoSendingMessageWithDisableMessageIdHintTestImpl(true);
        }

        private void DoSendingMessageWithDisableMessageIdHintTestImpl(bool existingId)
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                List<Amqp.Message> receivedMessages = new List<Amqp.Message>();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor(receivedMessages);
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                string text = "myMessage";
                ITextMessage message = session.CreateTextMessage(text);

                Assert.IsNull(message.NMSMessageId);

                if (existingId)
                {
                    string existingMessageId = "ID:this-should-be-overwritten-in-send";
                    message.NMSMessageId = existingMessageId;
                    Assert.AreEqual(existingMessageId, message.NMSMessageId);
                }

                producer.DisableMessageID = true;

                producer.Send(message);

                Assert.IsNull(message.NMSMessageId);

                Assert.IsTrue(receivedMessages.Any());
                Assert.AreEqual(message.NMSMessageId, receivedMessages.First().Properties.MessageId);

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestRemotelyCloseProducer()
        {
            ManualResetEvent producentClosed = new ManualResetEvent(false);
            Mock<INmsConnectionListener> mockConnectionListener = new Mock<INmsConnectionListener>();
            mockConnectionListener
                .Setup(listener => listener.OnProducerClosed(It.IsAny<NmsMessageProducer>(), It.IsAny<Exception>()))
                .Callback(() => { producentClosed.Set(); });

            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor();
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                NmsConnection connection = (NmsConnection) EstablishConnection();
                connection.AddConnectionListener(mockConnectionListener.Object);

                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                testLinkProcessor.Producer.Link.Close();

                Assert.True(producentClosed.WaitOne(TimeSpan.FromMilliseconds(1000)));
                Assert.That(() => producer.DisableMessageID, Throws.Exception.InstanceOf<IllegalStateException>());

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestSendWhenLinkCreditIsZeroAndTimeout()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                testAmqpPeer.RegisterLinkProcessor(new MockLinkProcessor(context =>
                {
                    context.Complete(new TestLinkEndpoint(new List<Amqp.Message>()), 0);
                }));

                IConnection connection = EstablishConnection("nms.sendTimeout=500");
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                string text = "myMessage";
                ITextMessage message = session.CreateTextMessage(text);

                Assert.Catch<Exception>(() => producer.Send(message));

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test, Timeout(2000)]
        public void TestSendTimesOutWhenNoDispositionArrives()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                testAmqpPeer.RegisterLinkProcessor(new MockLinkProcessor(context =>
                {
                    context.Complete(new MockLinkEndpoint(onDispositionHandler: dispositionContext =>
                    {
                        dispositionContext.Complete();
                        
                    }, onMessageHandler: messageContext =>
                    {
                        // do nothing
                    }), 1);
                }));

                IConnection connection = EstablishConnection("nms.sendTimeout=500");
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                string text = "myMessage";
                ITextMessage message = session.CreateTextMessage(text);

                Assert.Catch<Exception>(() => producer.Send(message));

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestSendWorksWhenConnectionNotStarted()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                List<Amqp.Message> receivedMessages = new List<Amqp.Message>();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor(receivedMessages);
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                string text = "myMessage";
                ITextMessage message = session.CreateTextMessage(text);

                Assert.IsNull(message.NMSMessageId);

                producer.Send(message);                

                Assert.IsTrue(receivedMessages.Any());

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        [Test]
        public void TestSendWorksAfterConnectionStopped()
        {
            using (TestAmqpPeer testAmqpPeer = new TestAmqpPeer(Address, User, Password))
            {
                testAmqpPeer.Open();
                List<Amqp.Message> receivedMessages = new List<Amqp.Message>();
                TestLinkProcessor testLinkProcessor = new TestLinkProcessor(receivedMessages);
                testAmqpPeer.RegisterLinkProcessor(testLinkProcessor);

                IConnection connection = EstablishConnection();
                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IQueue destination = session.GetQueue("myQueue");
                IMessageProducer producer = session.CreateProducer(destination);

                string text = "myMessage";
                ITextMessage message = session.CreateTextMessage(text);

                Assert.IsNull(message.NMSMessageId);
                
                connection.Stop();

                producer.Send(message);                

                Assert.IsTrue(receivedMessages.Any());

                producer.Close();
                session.Close();
                connection.Close();
            }
        }

        private IConnection EstablishConnection(string parameters = null)
        {
            NmsConnectionFactory factory = new NmsConnectionFactory(CreatePeerUri(parameters));
            return factory.CreateConnection(User, Password);
        }
        
        private string CreatePeerUri(string parameters = null)
        {
            return Address + (parameters != null ? "?" + parameters : "");
        }
    }
}