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
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS;
using NUnit.Framework;

namespace NMS.AMQP.Test
{
    [TestFixture]
    public class NmsMessageProducerTest : AmqpTestSupport
    {
        [Test, Timeout(60_000)]
        public void TestDeliveryDelay()
        {
            PurgeQueue(TimeSpan.FromMilliseconds(500));

            var deliveryDelay = TimeSpan.FromSeconds(7);
            
            Connection = CreateAmqpConnection();
            Connection.Start();

            ISession session = Connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IQueue queue = session.GetQueue(TestName);
            IMessageProducer producer = session.CreateProducer(queue);
            producer.DeliveryDelay = deliveryDelay;

            DateTime? receivingTime = null;
            IMessageConsumer consumer = session.CreateConsumer(queue);
            var receivingTask = Task.Run(() =>
            {
                while (true)
                {
                    var message = consumer.Receive(TimeSpan.FromMilliseconds(100));
                    if (message != null && message.Body<string>() == "Hello")
                    {
                        receivingTime = DateTime.Now;
                        return;
                    }
                }
            });
            
            
            DateTime sendTime = DateTime.Now;
            ITextMessage message = session.CreateTextMessage("Hello");
            producer.Send(message, MsgDeliveryMode.Persistent, MsgPriority.Normal, TimeSpan.Zero);

            // Wait that delivery delay
            Thread.Sleep(deliveryDelay);

            receivingTask.Wait(TimeSpan.FromSeconds(20)); // make sure its done

            var measuredDelay = (receivingTime.Value - sendTime);
            
            Assert.Greater(measuredDelay.TotalMilliseconds, deliveryDelay.TotalMilliseconds* 0.5);
            Assert.Less(measuredDelay.TotalMilliseconds, deliveryDelay.TotalMilliseconds*1.5);
        }

      
    }
}