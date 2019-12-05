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

namespace Apache.NMS.AMQP
{
    public class NmsMessageConsumer : IMessageConsumer
    {
        private readonly object syncRoot = new object();
        private readonly AcknowledgementMode acknowledgementMode;
        private readonly AtomicBool closed = new AtomicBool();
        private readonly MessageDeliveryTask deliveryTask;
        private readonly PriorityMessageQueue messageQueue = new PriorityMessageQueue();
        private readonly AtomicBool started = new AtomicBool();

        private Exception failureCause;

        public NmsMessageConsumer(NmsConsumerId consumerId, NmsSession session, IDestination destination, string selector, bool noLocal) : this(consumerId, session, destination, null, selector, noLocal)
        {
        }

        protected NmsMessageConsumer(NmsConsumerId consumerId, NmsSession session, IDestination destination, string name, string selector, bool noLocal)
        {
            Session = session;
            acknowledgementMode = session.AcknowledgementMode;

            if (destination.IsTemporary)
            {
                session.Connection.CheckConsumeFromTemporaryDestination((NmsTemporaryDestination) destination);
            }

            Info = new NmsConsumerInfo(consumerId)
            {
                Destination = destination,
                Selector = selector,
                NoLocal = noLocal,
                SubscriptionName = name,
                LocalMessageExpiry = Session.Connection.ConnectionInfo.LocalMessageExpiry,
                IsDurable = IsDurableSubscription
            };
            deliveryTask = new MessageDeliveryTask(this);
            
            Session.Connection.CreateResource(Info).ConfigureAwait(false).GetAwaiter().GetResult();
            
            Session.Add(this);

            if (Session.IsStarted)
                Start();
        }

        public NmsSession Session { get; }
        public NmsConsumerInfo Info { get; }
        public IDestination Destination => Info.Destination;

        protected virtual bool IsDurableSubscription => false;

        public void Dispose()
        {
            try
            {
                Close();
            }
            catch (Exception ex)
            {
                Tracer.DebugFormat("Caught exception while disposing {0} {1}. Exception {2}", GetType().Name, Info, ex);
            }
        }

        public void Close()
        {
            if (closed)
                return;

            lock (syncRoot)
            {
                Shutdown(null);
                Session.Connection.DestroyResource(Info).ConfigureAwait(false).GetAwaiter().GetResult();
            }
        }

        public ConsumerTransformerDelegate ConsumerTransformer { get; set; }

        event MessageListener IMessageConsumer.Listener
        {
            add
            {
                CheckClosed();
                lock (syncRoot)
                {
                    Listener += value;
                    DrainMessageQueueToListener();
                }
            }
            remove
            {
                lock (syncRoot)
                {
                    Listener -= value;
                }
            }
        }

        public IMessage Receive()
        {
            CheckClosed();
            CheckMessageListener();

            while (true)
            {
                if (started)
                {
                    return ReceiveInternal(-1);
                }
            }
        }

        public IMessage ReceiveNoWait()
        {
            CheckClosed();
            CheckMessageListener();

            return started ? ReceiveInternal(0) : null;
        }

        public IMessage Receive(TimeSpan timeout)
        {
            CheckClosed();
            CheckMessageListener();

            int timeoutInMilliseconds = (int) timeout.TotalMilliseconds;

            if (started)
            {
                return ReceiveInternal(timeoutInMilliseconds);
            }

            long deadline = GetDeadline(timeoutInMilliseconds);

            while (true)
            {
                timeoutInMilliseconds = (int) (deadline - DateTime.UtcNow.Ticks / 10_000L);
                if (timeoutInMilliseconds < 0)
                {
                    return null;
                }

                if (started)
                {
                    return ReceiveInternal(timeoutInMilliseconds);
                }
            }
        }

        private void CheckMessageListener()
        {
            if (HasMessageListener())
            {
                throw new IllegalStateException("Cannot synchronously receive a message when a MessageListener is set");
            }
        }

        private void CheckClosed()
        {
            if (closed)
            {
                if (failureCause == null)
                    throw new IllegalStateException("The MessageConsumer is closed");
                else
                    throw new IllegalStateException("The MessageConsumer was closed due to an unrecoverable error.", failureCause);
            }
        }

        private event MessageListener Listener;

        public Task Init()
        {
            return Session.Connection.StartResource(Info);
        }

        public void OnInboundMessage(InboundMessageDispatch envelope)
        {
            if (Tracer.IsDebugEnabled)
            {
                Tracer.Debug($"Message {envelope.Message.NMSMessageId} passed to consumer {Info.Id}");
            }

            SetAcknowledgeCallback(envelope);

            if (envelope.EnqueueFirst)
                messageQueue.EnqueueFirst(envelope);
            else
                messageQueue.Enqueue(envelope);

            if (Session.IsStarted && Listener != null)
            {
                Session.EnqueueForDispatch(deliveryTask);
            }
        }

        private void DeliverNextPending()
        {
            if (Tracer.IsDebugEnabled)
            {
                Tracer.Debug($"{Info.Id} is about to deliver next pending message.");
            }
            
            if (Session.IsStarted && started && Listener != null)
            {
                lock (syncRoot)
                {
                    try
                    {
                        if (started && Listener != null)
                        {
                            var envelope = messageQueue.DequeueNoWait();
                            if (envelope == null)
                            {
                                if (Tracer.IsDebugEnabled)
                                {
                                    Tracer.Debug($"No message available for delivery.");
                                }

                                return;
                            }

                            if (IsMessageExpired(envelope))
                            {
                                if (Tracer.IsDebugEnabled)
                                {
                                    Tracer.Debug($"{Info.Id} filtered expired message: {envelope.Message.NMSMessageId}");
                                }

                                DoAckExpired(envelope);
                            }
                            else if (IsRedeliveryExceeded(envelope))
                            {
                                if (Tracer.IsDebugEnabled)
                                {
                                    Tracer.Debug($"{Info.Id} filtered message with excessive redelivery count: {envelope.RedeliveryCount.ToString()}");
                                }

                                // TODO: Apply redelivery policy
                                DoAckExpired(envelope);
                            }
                            else
                            {
                                bool deliveryFailed = false;
                                bool autoAckOrDupsOk = acknowledgementMode == AcknowledgementMode.AutoAcknowledge || acknowledgementMode == AcknowledgementMode.DupsOkAcknowledge;

                                if (autoAckOrDupsOk)
                                    DoAckDelivered(envelope);
                                else
                                    AckFromReceive(envelope);

                                try
                                {
                                    Listener.Invoke(envelope.Message.Copy());
                                }
                                catch (Exception)
                                {
                                    deliveryFailed = true;
                                }

                                if (autoAckOrDupsOk)
                                {
                                    if (!deliveryFailed)
                                        DoAckConsumed(envelope);
                                    else
                                        DoAckReleased(envelope);
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        // TODO - There are two cases when we can get an error here:
                        // 1) error returned from the attempted ACK that was sent
                        // 2) error while attempting to copy the incoming message.
                        //
                        // We need to decide how to respond to these, but definitely we cannot
                        // let this error propagate as it could take down the SessionDispatcher
                        Session.Connection.OnAsyncException(e);
                    }
                }
            }
        }

        private bool IsMessageExpired(InboundMessageDispatch envelope)
        {
            NmsMessage message = envelope.Message;
            return Info.LocalMessageExpiry && message.IsExpired();
        }

        private bool IsRedeliveryExceeded(InboundMessageDispatch envelope)
        {
            Tracer.DebugFormat("Checking envelope with {0} redeliveries", envelope.RedeliveryCount);
            IRedeliveryPolicy redeliveryPolicy = Session.Connection.RedeliveryPolicy;
            if (redeliveryPolicy != null && redeliveryPolicy.MaximumRedeliveries >= 0)
            {
                return envelope.RedeliveryCount > redeliveryPolicy.MaximumRedeliveries;
            }

            return false;
        }

        private void DoAckReleased(InboundMessageDispatch envelope)
        {
            Session.AcknowledgeIndividual(AckType.RELEASED, envelope);
        }

        private IMessage ReceiveInternal(int timeout)
        {
            try
            {
                long deadline = 0;
                if (timeout > 0)
                {
                    deadline = GetDeadline(timeout);
                }

                while (true)
                {
                    if (Tracer.IsDebugEnabled)
                    {
                        Tracer.Debug("Trying to dequeue next message.");
                    }

                    InboundMessageDispatch envelope = messageQueue.Dequeue(timeout);

                    if (failureCause != null)
                        throw NMSExceptionSupport.Create(failureCause);

                    if (envelope == null)
                        return null;

                    if (IsMessageExpired(envelope))
                    {
                        if (Tracer.IsDebugEnabled)
                        {
                            Tracer.Debug($"{Info.Id} filtered expired message: {envelope.Message.NMSMessageId}");
                        }

                        DoAckExpired(envelope);

                        if (timeout > 0)
                            timeout = (int) Math.Max(deadline - DateTime.UtcNow.Ticks / 10_000L, 0);
                    }
                    else if (IsRedeliveryExceeded(envelope))
                    {
                        if (Tracer.IsDebugEnabled)
                        {
                            Tracer.Debug($"{Info.Id} filtered message with excessive redelivery count: {envelope.RedeliveryCount.ToString()}");
                        }

                        // TODO: Apply redelivery policy
                        DoAckExpired(envelope);
                    }
                    else
                    {
                        if (Tracer.IsDebugEnabled)
                        {
                            Tracer.Debug($"{Info.Id} received message {envelope.Message.NMSMessageId}.");
                        }

                        AckFromReceive(envelope);
                        return envelope.Message.Copy();
                    }
                }
            }
            catch (NMSException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw ExceptionSupport.Wrap(ex, "Receive failed");
            }
        }

        private static long GetDeadline(int timeout)
        {
            return DateTime.UtcNow.Ticks / 10_000L + timeout;
        }

        private void AckFromReceive(InboundMessageDispatch envelope)
        {
            if (envelope?.Message != null)
            {
                NmsMessage message = envelope.Message;
                if (message.NmsAcknowledgeCallback != null)
                {
                    DoAckDelivered(envelope);
                }
                else
                {
                    DoAckConsumed(envelope);
                }
            }
        }

        private void DoAckDelivered(InboundMessageDispatch envelope)
        {
            Session.Acknowledge(AckType.DELIVERED, envelope);
        }

        private void DoAckConsumed(InboundMessageDispatch envelope)
        {
            Session.Acknowledge(AckType.ACCEPTED, envelope);
        }

        private void DoAckExpired(InboundMessageDispatch envelope)
        {
            Session.Acknowledge(AckType.MODIFIED_FAILED_UNDELIVERABLE, envelope);
        }

        private void SetAcknowledgeCallback(InboundMessageDispatch envelope)
        {
            if (acknowledgementMode == AcknowledgementMode.ClientAcknowledge)
            {
                envelope.Message.NmsAcknowledgeCallback = new NmsAcknowledgeCallback(Session);
            }
            else if (Session.IsIndividualAcknowledge())
            {
                envelope.Message.NmsAcknowledgeCallback = new NmsAcknowledgeCallback(Session, envelope);
            }
        }

        public bool HasMessageListener()
        {
            return Listener != null;
        }

        public void Shutdown(Exception exception)
        {
            if (closed.CompareAndSet(false, true))
            {
                if (Tracer.IsDebugEnabled)
                {
                    Tracer.Debug("Shutting down NmsMessageConsumer.");
                }

                failureCause = exception;
                Session.Remove(this);
                started.Set(false);
                messageQueue.Dispose();
            }
            else
            {
                if (Tracer.IsDebugEnabled)
                {
                    Tracer.Debug("NmsMessageConsumer already closed.");
                }
            }
        }

        public void Start()
        {
            if (started.CompareAndSet(false, true))
            {
                DrainMessageQueueToListener();
            }
        }

        private void DrainMessageQueueToListener()
        {
            if (Listener != null && Session.IsStarted)
            {
                int size = messageQueue.Count;
                for (int i = 0; i < size; i++)
                {
                    Session.EnqueueForDispatch(deliveryTask);
                }
            }
        }

        public Task OnConnectionRecovery(IProvider provider)
        {
            return provider.CreateResource(Info);
        }

        public async Task OnConnectionRecovered(IProvider provider)
        {
            await provider.StartResource(Info).ConfigureAwait(false);
            DrainMessageQueueToListener();
        }

        public void Stop()
        {
            lock (syncRoot)
            {
                started.Set(false);
            }
        }

        public bool IsDestinationInUse(NmsTemporaryDestination destination)
        {
            return Equals(Info.Destination, destination);
        }

        public void OnConnectionInterrupted()
        {
            messageQueue.Clear();
        }

        public void SuspendForRollback()
        {
            Stop();

            try
            {
                Session.Connection.StopResource(Info).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            finally
            {
                if (Session.TransactionContext.IsActiveInThisContext(Info.Id))
                {
                    messageQueue.Clear();
                }
            }
        }

        public void ResumeAfterRollback()
        {
            Start();
            StartConsumerResource();
        }

        private void StartConsumerResource()
        {
            try
            {
                Session.Connection.StartResource(Info).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            catch (NMSException)
            {
                Session.Remove(this);
                throw;
            }
        }

        public class MessageDeliveryTask
        {
            private readonly NmsMessageConsumer consumer;

            public MessageDeliveryTask(NmsMessageConsumer consumer)
            {
                this.consumer = consumer;
            }

            public void DeliverNextPending()
            {
                consumer.DeliverNextPending();
            }
        }
    }
}