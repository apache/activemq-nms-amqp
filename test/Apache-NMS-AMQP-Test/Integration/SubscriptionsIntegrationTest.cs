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
using System.Net;
using Apache.NMS;
using Apache.NMS.AMQP;
using NUnit.Framework;
using Test.Amqp;
using List = Amqp.Types.List;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class SubscriptionsIntegrationTest
    {
        private static readonly string User = "USER";
        private static readonly string Password = "PASSWORD";
        private static readonly string Address = "amqp://127.0.0.1:5672";
        private static readonly IPEndPoint IPEndPoint = new IPEndPoint(IPAddress.Any, 5672);
        
        [Test]
        public void TestUnsubscribeDurableSubWhileActiveThenInactive()
        {
            using (var testListener = new TestListener(IPEndPoint))
            {
                testListener.Open();
                List result = null;
                testListener.RegisterTarget(TestPoint.Detach, (stream, channel, fields) =>
                {
                    TestListener.FRM(stream, 0x16UL, 0, channel, fields[0], false);
                    result = fields;
                    return TestOutcome.Stop;
                });
                
                IConnection connection = EstablishConnection();
                ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

                ITopic dest = session.GetTopic("myTopic");
                String subscriptionName = "mySubscription";

                IMessageConsumer consumer = session.CreateDurableConsumer(dest, subscriptionName, null, false);
                Assert.NotNull(consumer);

                // Now try to unsubscribe, should fail
                Assert.Catch<NMSException>(() => session.DeleteDurableConsumer(subscriptionName));
                
                consumer.Close();
                
                // Try to unsubscribe again, should work now
                session.DeleteDurableConsumer(subscriptionName);
                
                session.Close();
                connection.Close();
                
                // Assert that closed field is set to true
                Assert.IsTrue((bool) result[1]);
            }
        }
        
        private IConnection EstablishConnection()
        {
            NmsConnectionFactory factory = new NmsConnectionFactory(Address);
            IConnection connection = factory.CreateConnection(User, Password);
            connection.Start();
            return connection;
        }
    }
}