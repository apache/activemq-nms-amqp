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

using System.Threading.Tasks;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Provider;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP
{
    /// <summary>
    /// A Transaction Context is used to track and manage the state of a
    /// Transaction in a JMS Session object.
    /// </summary>
    internal interface INmsTransactionContext
    {
        /// <summary>
        /// Allows the context to intercept a message acknowledgement and perform any
        /// additional logic prior to the acknowledge being forwarded onto the connection.
        /// </summary>
        /// <param name="envelope">The envelope that contains the message to be acknowledged.</param>
        /// <param name="ackType">The acknowledgement type being requested.</param>
        Task Acknowledge(InboundMessageDispatch envelope, AckType ackType);
        
        /// <summary>
        /// Start a transaction if none is currently active.
        /// </summary>
        Task Begin();

        /// <summary>
        /// Allows the context to intercept and perform any additional logic
        /// prior to a message being sent on to the connection and subsequently
        /// the remote peer.
        /// </summary>
        /// <param name="envelope">The envelope that contains the message to be sent.</param>
        Task Send(OutboundMessageDispatch envelope);
        
        /// <summary>
        /// Rolls back any work done in this transaction and releases any locks
        /// currently held.  If the current transaction is in a failed state this
        /// resets that state and initiates a new transaction via a begin call.
        /// </summary>
        Task Rollback();
        
        /// <summary>
        /// Commits all work done in this transaction and releases any locks
        /// currently held.  If the transaction is in a failed state this method
        /// throws an exception to indicate that the transaction has failed and
        /// will be rolled back a new transaction is started via a begin call.
        /// </summary>
        /// <returns></returns>
        Task Commit();

        /// <summary>
        /// Rolls back any work done in this transaction and releases any locks
        /// currently held. This method will not start a new transaction and no new
        /// transacted work should be done using this transaction.
        /// </summary>
        Task Shutdown();

        /// <summary>
        /// Signals that the connection that was previously established has been lost and the
        /// listener should alter its state to reflect the fact that there is no active connection.
        /// </summary>
        void OnConnectionInterrupted();

        /// <summary>
        /// Called when the connection to the remote peer has been lost and then a new
        /// connection established.  The context should perform any necessary processing
        /// recover and reset its internal state.
        /// </summary>
        Task OnConnectionRecovery(IProvider provider);

        /// <summary>
        /// Allows a resource to query the transaction context to determine if it has pending
        /// work in the current transaction.
        /// </summary>
        bool IsActiveInThisContext(INmsResourceId infoId);
        
        event SessionTxEventDelegate TransactionStartedListener;
        event SessionTxEventDelegate TransactionCommittedListener;
        event SessionTxEventDelegate TransactionRolledBackListener;
    }
}