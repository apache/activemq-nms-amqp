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