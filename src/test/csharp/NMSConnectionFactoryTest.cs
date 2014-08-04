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
using System.Net.Sockets;
using Apache.NMS.Test;
using NUnit.Framework;

namespace Apache.NMS.Amqp.Test
{
    [TestFixture]
    public class NMSConnectionFactoryTest
    {
        // These tests assume that a broker is running on amqp port 5672.
        [Test]
        // These cases should be accepted
        [TestCase("amqp:localhost")]
        [TestCase("amqp:tcp:localhost")]
        [TestCase("amqp:user/pass@localhost")]
        [TestCase("amqp:user/pass@tcp:localhost")]
        [TestCase("amqp:user/pass@localhost:5672")]
        [TestCase("amqp:user/pass@tcp:localhost:5672")]
        [TestCase("amqp:[::1]")]
        [TestCase("amqp:tcp:[::1]")]
        [TestCase("amqp:user/pass@[::1]")]
        [TestCase("amqp:user/pass@tcp:[::1]")]
        [TestCase("amqp:user/pass@tcp:[::1]:5672")]

        // This case fails to connect to two hosts but then connects to the third
        [TestCase("amqp:bogushost1,bogushost2:9988,localhost:5672")]

        // This test case causes NUnit to assert but is OK for a host name
        //[TestCase("amqp:tcp:host-._~%ff%23:42")]

        // These test cases fail to parse the URI
        [TestCase("amqp:tcp:", ExpectedException = typeof(ConnectionClosedException))]
        [TestCase("amqp:", ExpectedException = typeof(ConnectionClosedException))]
        [TestCase("amqp::5672", ExpectedException = typeof(ConnectionClosedException))]

        // These test cases pass parsing the URI but fail to connect
        [TestCase("amqp:tcp:localhost:42",               ExpectedException = typeof(ConnectionClosedException))]
        [TestCase("amqp:hostlocal:42",                   ExpectedException = typeof(ConnectionClosedException))]
        [TestCase("amqp:tcp",                            ExpectedException = typeof(ConnectionClosedException))]
        [TestCase("amqp:876",                            ExpectedException = typeof(ConnectionClosedException))]
        [TestCase("amqp:tcp:567",                        ExpectedException = typeof(ConnectionClosedException))]
        [TestCase("amqp:[::]",                           ExpectedException = typeof(ConnectionClosedException))]
        [TestCase("amqp:[::127.0.0.1]",                  ExpectedException = typeof(ConnectionClosedException))]
        [TestCase("amqp:[2002::222:68ff:fe0b:e61a]",     ExpectedException = typeof(ConnectionClosedException))]
        [TestCase("amqp:[2002::222:68ff:fe0b:e61a]:123", ExpectedException = typeof(ConnectionClosedException))]
        [TestCase("amqp:user@tcp:badhost:123",           ExpectedException = typeof(ConnectionClosedException))]
        [TestCase("amqp:user/pass@badhost",              ExpectedException = typeof(ConnectionClosedException))]
        [TestCase("amqp:user@badhost",                   ExpectedException = typeof(ConnectionClosedException))]

        public void TestURI(string connectionURI)
        {
            //Apache.NMS.Tracer.Trace = new Apache.NMS.Amqp.Test.NmsConsoleTracer();
            Tracer.Debug("Connecting to URI: " + connectionURI);
            NMSConnectionFactory factory = new NMSConnectionFactory(connectionURI);
            Assert.IsNotNull(factory);
            Assert.IsNotNull(factory.ConnectionFactory);
            using (IConnection connection = factory.CreateConnection("", ""))
            {
                Assert.IsNotNull(connection);

                // The connection URI values are not used when a factory is created
                // nor when a connection is created. 
                // They are used when the connection is started.
                connection.Start();
            }
        }

    }
}
