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
using System.Threading.Tasks;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Provider;
using Apache.NMS.AMQP.Util;
using Apache.NMS.Util;

namespace NMS.AMQP.Test.Provider.Mock
{
    public class MockProvider : IProvider
    {
        private Atomic<bool> closed = new Atomic<bool>();
        private readonly MockRemotePeer context;
        private IProviderListener listener;

        public MockProvider(Uri remoteUri, MockProviderConfiguration configuration, MockRemotePeer context)
        {
            RemoteUri = remoteUri;
            Stats = new MockProviderStats(context?.ContextStats);
            Configuration = configuration;
            this.context = context;
        }

        public MockProviderConfiguration Configuration { get; }
        public MockProviderStats Stats { get; }

        public long SendTimeout { get; }
        public Uri RemoteUri { get; }

        public void Start()
        {
            if (Configuration.FailOnStart)
                throw new Exception();
        }

        public async Task Connect(NmsConnectionInfo connectionInfo)
        {
            Stats.RecordConnectAttempt();

            if (Configuration.FailOnConnect)
                throw new Exception();
            
            await Task.CompletedTask;
        }

        public void Close()
        {
            if (closed.CompareAndSet(false, true))
            {
                Stats.RecordCloseAttempt();

                if (Configuration.FailOnClose)
                    throw new Exception();
            }
        }

        public void SetProviderListener(IProviderListener providerListener)
        {
            listener = providerListener;
        }

        public Task CreateResource(INmsResource resourceInfo)
        {
            Stats.RecordCreateResourceCall(resourceInfo.GetType());
            return Task.CompletedTask;
        }

        public Task DestroyResource(INmsResource resourceInfo)
        {
            Stats.RecordDestroyResourceCall(resourceInfo.GetType());
            return Task.CompletedTask;
        }

        public Task StartResource(INmsResource resourceInfo)
        {
            return Task.CompletedTask;
        }

        public Task StopResource(INmsResource resourceInfo)
        {
            throw new NotImplementedException();
        }

        public Task Recover(NmsSessionId sessionId)
        {
            Stats.RecordRecoverCalls();
            return Task.CompletedTask;
        }

        public Task Acknowledge(NmsSessionId sessionId, AckType ackType)
        {
            throw new NotImplementedException();
        }

        public Task Acknowledge(InboundMessageDispatch envelope, AckType ackType)
        {
            throw new NotImplementedException();
        }

        public INmsMessageFactory MessageFactory { get; } = null;
        public Task Send(OutboundMessageDispatch envelope)
        {
            return Task.CompletedTask;
        }

        public Task Unsubscribe(string name)
        {
            throw new NotImplementedException();
        }

        public Task Rollback(NmsTransactionInfo transactionInfo, NmsTransactionInfo nextTransactionInfo)
        {
            throw new NotImplementedException();
        }

        public Task Commit(NmsTransactionInfo transactionInfo, NmsTransactionInfo nextTransactionInfo)
        {
            throw new NotImplementedException();
        }

        public InboundMessageDispatch ReceiveMessage(NmsConsumerInfo consumerInfo, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }
    }
}