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

namespace Apache.NMS.AMQP.Provider
{
    public interface IProviderListener
    {
        void OnInboundMessage(InboundMessageDispatch envelope);

        void OnConnectionFailure(NMSException exception);

        Task OnConnectionRecovery(IProvider provider);

        /// <summary>
        /// Called to indicate that the underlying connection to the Broker has been established
        /// for the first time.  For a fault tolerant provider this event should only ever be
        /// triggered once with the interruption and recovery events following on for future
        /// </summary>
        /// <param name="remoteUri">The Uri of the Broker that the client has now connected to.</param>
        void OnConnectionEstablished(Uri remoteUri);

        /// <summary>
        /// Called to indicate that a connection to the Broker has been reestablished and
        /// that all recovery operations have succeeded and the connection will now be
        /// transitioned to a recovered state.  This method gives the listener a chance
        /// so send any necessary post recovery commands such as consumer start or message
        /// pull for a zero prefetch consumer etc.
        /// </summary>
        /// <param name="provider">The new Provider instance that will become active after the state has been recovered.</param>
        Task OnConnectionRecovered(IProvider provider);

        /// <summary>
        /// Called to signal that all recovery operations are now complete and the Provider
        /// is again in a normal connected state.
        ///
        /// It is considered a programming error to allow any exceptions to be thrown from
        /// this notification method.
        /// </summary>
        /// <param name="remoteUri">The Uri of the Broker that the client has now connected to.</param>
        void OnConnectionRestored(Uri remoteUri);

        void OnResourceClosed(INmsResource resource, Exception error);

        /// <summary>
        /// Called from a fault tolerant Provider instance to signal that the underlying
        /// connection to the Broker has been lost.  The Provider will attempt to reconnect
        /// following this event unless closed.
        ///
        /// It is considered a programming error to allow any exceptions to be thrown from
        /// this notification method.
        /// </summary>
        /// <param name="failedUri">The Uri of the Broker whose connection was lost.</param>
        void OnConnectionInterrupted(Uri failedUri);
    }
}