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
        Task Connect(NmsConnectionInfo connectionInfo);
        void Close();
        void SetProviderListener(IProviderListener providerListener);
        Task CreateResource(INmsResource resourceInfo);
        Task DestroyResource(INmsResource resourceInfo);
        Task StartResource(INmsResource resourceInfo);
        Task StopResource(INmsResource resourceInfo);
        Task Recover(NmsSessionId sessionId);
        Task Acknowledge(NmsSessionId sessionId, AckType ackType);
        Task Acknowledge(InboundMessageDispatch envelope, AckType ackType);
        INmsMessageFactory MessageFactory { get; }
        Task Send(OutboundMessageDispatch envelope);
        Task Unsubscribe(string name);
        Task Rollback(NmsTransactionInfo transactionInfo, NmsTransactionInfo nextTransactionInfo);
        Task Commit(NmsTransactionInfo transactionInfo, NmsTransactionInfo nextTransactionInfo);
    }
}