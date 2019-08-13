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
using Amqp.Listener;

namespace NMS.AMQP.Test.TestAmqp
{
    public class TestAmqpPeer : IDisposable
    {
        private readonly ContainerHost containerHost;
        private readonly Dictionary<string, TestMessageSource> messageSources = new Dictionary<string, TestMessageSource>();
        
        public TestAmqpPeer(string address, string user, string password)
        {
            Address = new Uri(address);
            containerHost = new ContainerHost(new[] { this.Address }, null, $"{user}:{password}");
            containerHost.RegisterRequestProcessor("TestRequestProcessor", new TestRequestProcessor());
        }
        
        public Uri Address { get; }

        public void Open()
        {
            containerHost.Open();
        }

        public void Close()
        {
            containerHost.Close();
        }

        public void Dispose()
        {
            Close();
        }

        public void RegisterMessageSource(string address)
        {
            var messageSource = new TestMessageSource();
            containerHost.RegisterMessageSource(address, messageSource);
            messageSources.Add(address, messageSource);
        }

        public void SendMessage(string address, string payload)
        {
            Amqp.Message message = new Amqp.Message(payload) { Properties = new Amqp.Framing.Properties { MessageId = Guid.NewGuid().ToString() } };
            SendMessage(address, message);
        }

        public void SendMessage(string address, Amqp.Message message)
        {
            if (messageSources.TryGetValue(address, out var messageSource))
            {
                messageSource.SendMessage(message);
            }
            else
            {
                messageSource = new TestMessageSource();
                messageSource.SendMessage(message);
                containerHost.RegisterMessageSource(address, messageSource);
                messageSources.Add(address, messageSource);
            }
        }

        public void RegisterLinkProcessor(ILinkProcessor linkProcessor)
        {
            containerHost.RegisterLinkProcessor(linkProcessor);
        }

        public void RegisterMessageProcessor(string address, Action<MessageContext> handler)
        {
            containerHost.RegisterMessageProcessor(address, new TestMessageProcessor(handler));
        }

        public IEnumerable<Amqp.Message> ReleasedMessages => messageSources.Values.SelectMany(x => x.ReleasedMessages);
        public IEnumerable<Amqp.Message> AcceptedMessages => messageSources.Values.SelectMany(x => x.AcceptedMessages);
    }
}