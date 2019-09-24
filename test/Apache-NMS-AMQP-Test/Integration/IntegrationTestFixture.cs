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

using Amqp.Framing;
using Amqp.Types;
using Apache.NMS;
using Apache.NMS.AMQP;
using NMS.AMQP.Test.TestAmqp;

namespace NMS.AMQP.Test.Integration
{
    public class IntegrationTestFixture
    {
        static IntegrationTestFixture()
        {
            Tracer.Trace = new NLogAdapter();
        }
        
        protected IConnection EstablishConnection(TestAmqpPeer testPeer, string optionsString = null, Symbol[] serverCapabilities = null, Fields serverProperties = null, bool setClientId = true)
        {
            testPeer.ExpectSaslPlain("guest", "guest");
            testPeer.ExpectOpen(serverCapabilities: serverCapabilities, serverProperties: serverProperties);

            // Each connection creates a session for managing temporary destinations etc.
            testPeer.ExpectBegin();

            var remoteUri = BuildUri(testPeer, optionsString);
            var connectionFactory = new NmsConnectionFactory(remoteUri);
            var connection = connectionFactory.CreateConnection("guest", "guest");
            if (setClientId)
            {
                // Set a clientId to provoke the actual AMQP connection process to occur.
                connection.ClientId = "ClientName";
            }

            return connection;
        }

        private static string BuildUri(TestAmqpPeer testPeer, string optionsString)
        {
            string baseUri = "amqp://127.0.0.1:" + testPeer.ServerPort.ToString();

            if (string.IsNullOrEmpty(optionsString)) 
                return baseUri;

            if (optionsString.StartsWith("?"))
                return baseUri + optionsString;
            else
                return baseUri + "?" + optionsString;

        }
        
        protected static Amqp.Message CreateMessageWithContent()
        {
            return new Amqp.Message() { BodySection = new AmqpValue() { Value = "content" } };
        }
        
        protected static Amqp.Message CreateMessageWithNullContent()
        {
            return new Amqp.Message() { BodySection = new AmqpValue() { Value = null } };
        }
    }
}