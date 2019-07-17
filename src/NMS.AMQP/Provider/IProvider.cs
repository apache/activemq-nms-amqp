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
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider
{
    public interface IProvider
    {
        long SendTimeout { get; }
        Uri RemoteUri { get; }
        void Start();
        Task Connect(ConnectionInfo connectionInfo);
        void Close();
        void SetProviderListener(IProviderListener providerListener);
        Task CreateResource(ResourceInfo resourceInfo);
        Task DestroyResource(ResourceInfo resourceInfo);
        Task StartResource(ResourceInfo resourceInfo);
        Task StopResource(ResourceInfo resourceInfo);
        Task Recover(Id sessionId);
        Task Acknowledge(Id sessionId, AckType ackType);
        Task Acknowledge(InboundMessageDispatch envelope, AckType ackType);
        INmsMessageFactory MessageFactory { get; }
        Task Send(OutboundMessageDispatch envelope);
        Task Unsubscribe(string name);
        Task Rollback(TransactionInfo transactionInfo, TransactionInfo nextTransactionInfo);
        Task Commit(TransactionInfo transactionInfo, TransactionInfo nextTransactionInfo);
    }
}