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
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Provider;
using Apache.NMS.AMQP.Util;
using Apache.NMS.AMQP.Util.Synchronization;
using Apache.NMS.Util;

namespace Apache.NMS.AMQP
{
    public class NmsMessageConsumer : IMessageConsumer
    {
        private readonly NmsSynchronizationMonitor syncRoot = new NmsSynchronizationMonitor();
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
                IsExplicitClientId = Session.Connection.ConnectionInfo.IsExplicitClientId,
                SubscriptionName = name,
                IsShared = IsSharedSubscription,
                IsDurable = IsDurableSubscription,
                IsBrowser =  IsBrowser,
                LocalMessageExpiry = Session.Connection.ConnectionInfo.LocalMessageExpiry,
                LinkCredit = Session.Connection.ConnectionInfo.PrefetchPolicy.GetLinkCredit(destination, IsBrowser, IsDurableSubscription),
                DeserializationPolicy = Session.Connection.ConnectionInfo.DeserializationPolicy.Clone()
            };
            deliveryTask = new MessageDeliveryTask(this);
        }

        public NmsSession Session { get; }
        public NmsConsumerInfo Info { get; }
        public IDestination Destination => Info.Destination;

        protected virtual bool IsDurableSubscription => false;
        
        protected virtual bool IsSharedSubscription => false;
        
        protected virtual bool IsBrowser => false;

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
            CloseAsync().GetAsyncResult();
        }

        public async Task CloseAsync()
        {
            if (closed)
                return;

            using(await syncRoot.LockAsync().Await())
            {
                Shutdown(null);
                await Session.Connection.DestroyResource(Info).Await();
            }
        }

        public ConsumerTransformerDelegate ConsumerTransformer { get; set; }

        public string MessageSelector => Info.Selector;
        
        event AsyncMessageListener IMessageConsumer.AsyncListener 
        {
            add
            {
                CheckClosed();
                using(syncRoot.Lock())
                {
                    AsyncListener += value;
                    DrainMessageQueueToListener();
                }
            }
            remove
            {
                using (syncRoot.LockAsync())
                {
                    AsyncListener -= value;
                }
            }
        }

        event MessageListener IMessageConsumer.Listener
        {
            add
            {
                CheckClosed();
                using(syncRoot.Lock())
                {
                    Listener += value;
                    DrainMessageQueueToListener();
                }
            }
            remove
            {
                using(syncRoot.Lock())
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
                    return ReceiveInternalAsync(-1).GetAsyncResult();
                }
            }
        }

        public async Task<IMessage> ReceiveAsync()
        {
            CheckClosed();
            CheckMessageListener();

            while (true)
            {
                if (started)
                {
                    return await ReceiveInternalAsync(-1).Await();
                }
            }
        }

        public T ReceiveBody<T>()
        {
            return ReceiveBodyAsync<T>().GetAsyncResult();
        }
        
        public Task<T> ReceiveBodyAsync<T>()
        {
            CheckClosed();
            CheckMessageListener();

            while (true)
            {
                if (started)
                {
                    return ReceiveBodyInternalAsync<T>(-1);
                }
            }
        }


        public IMessage ReceiveNoWait()
        {
            CheckClosed();
            CheckMessageListener();

            return started ? ReceiveInternalAsync(0).GetAsyncResult() : null;
        }
        
        public T ReceiveBodyNoWait<T>()
        {
            CheckClosed();
            CheckMessageListener();

            return started ? ReceiveBodyInternalAsync<T>(0).GetAsyncResult() : default;
        }

        public IMessage Receive(TimeSpan timeout)
        {
            return ReceiveAsync(timeout).GetAsyncResult();
        }
        
        public async Task<IMessage> ReceiveAsync(TimeSpan timeout)
        {
            CheckClosed();
            CheckMessageListener();
            
            int timeoutInMilliseconds = (int) timeout.TotalMilliseconds;

            if (started)
            {
                return await ReceiveInternalAsync(timeoutInMilliseconds).Await();
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
                    return await ReceiveInternalAsync(timeoutInMilliseconds).Await();
                }
            }
        }
        
        public T ReceiveBody<T>(TimeSpan timeout)
        {
            return ReceiveBodyAsync<T>(timeout).GetAsyncResult();
        }
        
        public async Task<T> ReceiveBodyAsync<T>(TimeSpan timeout)
        {
            CheckClosed();
            CheckMessageListener();

            int timeoutInMilliseconds = (int) timeout.TotalMilliseconds;

            if (started)
            {
                return await ReceiveBodyInternalAsync<T>(timeoutInMilliseconds).Await();
            }

            long deadline = GetDeadline(timeoutInMilliseconds);

            while (true)
            {
                timeoutInMilliseconds = (int) (deadline - DateTime.UtcNow.Ticks / 10_000L);
                if (timeoutInMilliseconds < 0)
                {
                    return default;
                }

                if (started)
                {
                    return await ReceiveBodyInternalAsync<T>(timeoutInMilliseconds).Await();
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

        private event AsyncMessageListener AsyncListener;

        public async Task Init()
        {
            await Session.Connection.CreateResource(Info).Await();
            
            Session.Add(this);

            if (Session.IsStarted)
                Start();
            
            await Session.Connection.StartResource(Info).Await();
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

            if (Session.IsStarted && HasMessageListener())
            {
                using (syncRoot.Exclude()) // Exclude lock for a time of dispatching, so it does not pass along to actionblock
                {
                    Session.EnqueueForDispatch(deliveryTask);
                }
            }
        }

        private async Task DeliverNextPendingAsync(CancellationToken cancellationToken)
        {
            if (Tracer.IsDebugEnabled)
            {
                Tracer.Debug($"{Info.Id} is about to deliver next pending message.");
            }
            
            if (Session.IsStarted && this.started && HasMessageListener())
            {
                using(await syncRoot.LockAsync().Await())
                {
                    try
                    {
                        if (this.started && HasMessageListener())
                        {
                            var envelope = messageQueue.DequeueNoWait();
                            if (envelope == null)
                            {
                                if (Tracer.IsDebugEnabled)
                                {
                                    Tracer.Debug("No message available for delivery.");
                                }

                                return;
                            }

                            if (IsMessageExpired(envelope))
                            {
                                if (Tracer.IsDebugEnabled)
                                {
                                    Tracer.Debug($"{Info.Id} filtered expired message: {envelope.Message.NMSMessageId}");
                                }

                                await DoAckExpiredAsync(envelope).Await();
                            }
                            else if (IsRedeliveryExceeded(envelope))
                            {
                                if (Tracer.IsDebugEnabled)
                                {
                                    Tracer.Debug($"{Info.Id} filtered message with excessive redelivery count: {envelope.RedeliveryCount.ToString()}");
                                }

                                // TODO: Apply redelivery policy
                                await DoAckExpiredAsync(envelope).Await();
                            }
                            else
                            {
                                bool deliveryFailed = false;
                                bool autoAckOrDupsOk = acknowledgementMode == AcknowledgementMode.AutoAcknowledge || acknowledgementMode == AcknowledgementMode.DupsOkAcknowledge;

                                if (autoAckOrDupsOk)
                                    await DoAckDeliveredAsync(envelope).Await();
                                else
                                    await AckFromReceiveAsync(envelope).Await();

                                try
                                {
                                    Listener?.Invoke(envelope.Message.Copy());
                                    if (AsyncListener != null)
                                    {
                                        await AsyncListener.Invoke(envelope.Message.Copy(), cancellationToken).Await();
                                    }
                                }
                                catch (Exception)
                                {
                                    deliveryFailed = true;
                                }

                                if (autoAckOrDupsOk)
                                {
                                    if (!deliveryFailed)
                                        await DoAckConsumedAsync(envelope).Await();
                                    else
                                        await DoAckReleasedAsync(envelope).Await();
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

                        // To let close the existing session/connection in error handler
                        using (Session.ExcludeCheckIsOnDeliveryExecutionFlow())
                        {
                            Session.Connection.OnAsyncException(e);
                        }
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

        private Task DoAckReleasedAsync(InboundMessageDispatch envelope)
        {
            return Session.AcknowledgeIndividualAsync(AckType.RELEASED, envelope);
        }

        
        private Task<IMessage> ReceiveInternalAsync(int timeout)
        {
            return ReceiveInternalBaseAsync(timeout, async envelope =>
            {
                IMessage message = envelope.Message.Copy();
                await AckFromReceiveAsync(envelope);
                return message;
            });
        }
        
        
        private Task<T> ReceiveBodyInternalAsync<T>(int timeout)
        {
            return ReceiveInternalBaseAsync<T>(timeout, async envelope =>
            {
                try
                {
                    T body = envelope.Message.Body<T>();
                    await AckFromReceiveAsync(envelope);
                    return body;
                }
                catch (MessageFormatException mfe)
                {
                    // Should behave as if receiveBody never happened in these modes.
                    if (acknowledgementMode == AcknowledgementMode.AutoAcknowledge ||
                        acknowledgementMode == AcknowledgementMode.DupsOkAcknowledge) {

                        envelope.EnqueueFirst = true;
                        OnInboundMessage(envelope);
                    }

                    throw mfe;
                }
            });
        }


        private async Task<T> ReceiveInternalBaseAsync<T>(int timeout, Func<InboundMessageDispatch, Task<T>> func)
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

                    InboundMessageDispatch envelope = await messageQueue.DequeueAsync(timeout).Await();

                    if (failureCause != null)
                        throw NMSExceptionSupport.Create(failureCause);

                    if (envelope == null)
                        return default;

                    if (IsMessageExpired(envelope))
                    {
                        if (Tracer.IsDebugEnabled)
                        {
                            Tracer.Debug($"{Info.Id} filtered expired message: {envelope.Message.NMSMessageId}");
                        }

                        await DoAckExpiredAsync(envelope).Await();

                        if (timeout > 0)
                            timeout = (int) Math.Max(deadline - DateTime.UtcNow.Ticks / 10_000L, 0);
                    }
                    else if (IsRedeliveryExceeded(envelope))
                    {
                        if (Tracer.IsDebugEnabled)
                        {
                            Tracer.Debug($"{Info.Id} filtered message with excessive redelivery count: {envelope.RedeliveryCount.ToString()}");
                        }

                        await DoAckExpiredAsync(envelope).Await();
                    }
                    else
                    {
                        if (Tracer.IsDebugEnabled)
                        {
                            Tracer.Debug($"{Info.Id} received message {envelope.Message.NMSMessageId}.");
                        }

                        return await func.Invoke(envelope);
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

        private async Task AckFromReceiveAsync(InboundMessageDispatch envelope)
        {
            if (envelope?.Message != null)
            {
                NmsMessage message = envelope.Message;
                if (message.NmsAcknowledgeCallback != null)
                {
                    await DoAckDeliveredAsync(envelope).Await();
                }
                else
                {
                    await DoAckConsumedAsync(envelope).Await();
                }
            }
        }

        private Task DoAckDeliveredAsync(InboundMessageDispatch envelope)
        {
            return Session.AcknowledgeAsync(AckType.DELIVERED, envelope);
        }

        private Task DoAckConsumedAsync(InboundMessageDispatch envelope)
        {
            return Session.AcknowledgeAsync(AckType.ACCEPTED, envelope);
        }

        private Task DoAckExpiredAsync(InboundMessageDispatch envelope)
        {
            if (Session.Connection.RedeliveryPolicy != null)
            {
                var dispositionType = Session.Connection.RedeliveryPolicy.GetOutcome(envelope.Message.NMSDestination);
                var ackType = LookupAckTypeForDisposition(dispositionType);
                return Session.AcknowledgeAsync(ackType, envelope);
            }
            else
            {
                return Session.AcknowledgeAsync(AckType.MODIFIED_FAILED_UNDELIVERABLE, envelope);
            }
        }

        private static AckType LookupAckTypeForDisposition(int dispositionType)
        {
            var ackType = (AckType) dispositionType;

            switch (ackType)
            {
                case AckType.ACCEPTED:
                case AckType.REJECTED:
                case AckType.RELEASED:
                case AckType.MODIFIED_FAILED:
                case AckType.MODIFIED_FAILED_UNDELIVERABLE:
                    return ackType;
                default:
                    throw new ArgumentOutOfRangeException(nameof(dispositionType), "Unknown disposition type");
            }
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
            return Listener != null || AsyncListener != null;
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
            if (HasMessageListener() && Session.IsStarted)
            {
                int size = messageQueue.Count;
                for (int i = 0; i < size; i++)
                {
                    using (syncRoot.Exclude()) // Exclude lock for a time of dispatching, so it does not pass along to actionblock
                    {
                        Session.EnqueueForDispatch(deliveryTask);
                    }
                }
            }
        }

        public Task OnConnectionRecovery(IProvider provider)
        {
            return provider.CreateResource(Info);
        }

        public async Task OnConnectionRecovered(IProvider provider)
        {
            await provider.StartResource(Info).Await();
            DrainMessageQueueToListener();
        }

        public void Stop()
        {
            using(syncRoot.Lock())
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

        public async Task SuspendForRollbackAsync()
        {
            Stop();

            try
            {
                await Session.Connection.StopResource(Info).Await();
            }
            finally
            {
                if (Session.TransactionContext.IsActiveInThisContext(Info.Id))
                {
                    messageQueue.Clear();
                }
            }
        }

        public async Task ResumeAfterRollbackAsync()
        {
            Start();
            await StartConsumerResourceAsync().Await();
        }

        private async Task StartConsumerResourceAsync()
        {
            try
            {
                await Session.Connection.StartResource(Info).Await();
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

            public Task DeliverNextPending(CancellationToken cancellationToken)
            {
                return consumer.DeliverNextPendingAsync(cancellationToken);
            }
        }
    }
}