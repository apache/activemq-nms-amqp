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
using System.Text;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Apache.NMS;
using NMS.AMQP.Util;
using NMS.AMQP.Util.Types.Queue;
using NMS.AMQP.Message.Cloak;

namespace NMS.AMQP
{
    /// <summary>
    /// MessageConsumer Implement the <see cref="Apache.NMS.IMessageConsumer"/> interface. 
    /// This class configures and manages an amqp receiver link.
    /// The Message consumer can be configured to receive message asynchronously or synchronously.
    /// </summary>
    class MessageConsumer : MessageLink, IMessageConsumer
    {
        // The Executor threshold limits the number of message listener dispatch events that can be on the session's executor at given time.
        private const int ExecutorThreshold = 2;
        private ConsumerInfo consumerInfo;
        private readonly string selector;
        private Apache.NMS.Util.Atomic<bool> hasStarted = new Apache.NMS.Util.Atomic<bool>(false);
        private MessageCallback OnInboundAMQPMessage;
        private IMessageQueue messageQueue;
        private LinkedList<IMessageDelivery> delivered;
        private System.Threading.ManualResetEvent MessageListenerInUseEvent = new System.Threading.ManualResetEvent(true);
        // pending Message delivery tasks counts the number of pending tasks on the Session's executor.
        // this should optimize the number of delivery task on the executor thread.
        private volatile int pendingMessageDeliveryTasks = 0;
        private volatile int MaxPendingTasks = 0;

        // stat counters useful for debuging
        // TODO create statistic container for counters maybe use ConsumerInfo?
        private int transportMsgCount = 0;
        private int messageDispatchCount = 0;

        #region Constructor

        internal MessageConsumer(Session ses, Destination dest) : this(ses, dest, null, null)
        {
        }

        internal MessageConsumer(Session ses, IDestination dest) : this(ses, dest, null, null)
        {
        }

        internal MessageConsumer(Session ses, IDestination dest, string name, string selector, bool noLocal = false) : base(ses, dest)
        {
            this.selector = selector;
            consumerInfo = new ConsumerInfo(ses.ConsumerIdGenerator.GenerateId());
            consumerInfo.SubscriptionName = name;
            consumerInfo.Selector = this.selector;
            consumerInfo.NoLocal = noLocal;
            Info = consumerInfo;
            messageQueue =  new PriorityMessageQueue();
            delivered = new LinkedList<IMessageDelivery>();
            Configure();
        }
        
        #endregion

        #region Private Properties

        protected new IReceiverLink Link
        {
            get { return base.Link as IReceiverLink; }
            
        }
        
        // IsBrowser is a stub for an inherited Brower subclass
        protected virtual bool IsBrowser { get { return false; } }

        #endregion

        #region Internal Properties

        internal Id ConsumerId { get { return this.Info.Id; } }

        internal virtual bool IsDurable {  get { return this.consumerInfo.SubscriptionName != null && this.consumerInfo.SubscriptionName.Length > 0; } }

        internal virtual bool HasSelector { get { return this.consumerInfo.Selector != null && this.consumerInfo.Selector.Length > 0; } }
        
        #endregion

        #region Private Methods

        private void AckReceived(IMessageDelivery delivery)
        {
            IMessageCloak cloak = delivery.Message.GetMessageCloak();
            if (cloak.AckHandler != null)
            {
                delivered.AddLast(delivery);
            }
            else
            {
                AckConsumed(delivery);
            }
        }
        
        private void AckConsumed(IMessageDelivery delivery)
        {
            Message.Message nmsMessage = delivery.Message;
            Tracer.DebugFormat("Consumer {0} Acking Accepted for Message {1} ", ConsumerId, nmsMessage.NMSMessageId);
            delivered.Remove(delivery);
            Amqp.Message amqpMessage = (nmsMessage.GetMessageCloak() as Message.AMQP.AMQPMessageCloak).AMQPMessage;
            this.Link.Accept(amqpMessage);
        }

        private void AckReleased(IMessageDelivery delivery)
        {
            Message.Message nmsMessage = delivery.Message;
            Tracer.DebugFormat("Consumer {0} Acking Released for Message {1} ", ConsumerId, nmsMessage.NMSMessageId);
            Amqp.Message amqpMessage = (nmsMessage.GetMessageCloak() as Message.AMQP.AMQPMessageCloak).AMQPMessage;
            this.Link.Release(amqpMessage);
        }

        private void AckRejected(IMessageDelivery delivery, NMSException ex)
        {
            Error err = new Error(NMSErrorCode.INTERNAL_ERROR);
            err.Description = ex.Message;
            AckRejected(delivery, err);
        }
        
        private void AckRejected(IMessageDelivery delivery, Error err = null)
        {
            Tracer.DebugFormat("Consumer {0} Acking Rejected for Message {1} with error {2} ", ConsumerId, delivery.Message.NMSMessageId, err?.ToString());
            Amqp.Message amqpMessage = (delivery.Message.GetMessageCloak() as Message.AMQP.AMQPMessageCloak).AMQPMessage;
            this.Link.Reject(amqpMessage, err);
        }

        private void AckModified(IMessageDelivery delivery, bool deliveryFailed, bool undeliverableHere = false)
        {
            Message.Message nmsMessage = delivery.Message;
            Tracer.DebugFormat("Consumer {0} Acking Modified for Message {1}{2}{3}.", ConsumerId, nmsMessage.NMSMessageId, 
                deliveryFailed ? " Delivery Failed" : "",
                undeliverableHere ? " Undeliveryable Here" : "");
            Amqp.Message amqpMessage = (nmsMessage.GetMessageCloak() as Message.AMQP.AMQPMessageCloak).AMQPMessage;
            //TODO use Link.Modified from amqpNetLite 2.0.0
            this.Link.Modify(amqpMessage, deliveryFailed, undeliverableHere, null);
        }

        private bool IsMessageRedeliveryExceeded(IMessageDelivery delivery)
        {
            Message.Message message = delivery.Message;
            IRedeliveryPolicy policy = this.Session.Connection.RedeliveryPolicy;
            if (policy != null && policy.MaximumRedeliveries >= 0)
            {
                IMessageCloak msgCloak = message.GetMessageCloak();
                return msgCloak.RedeliveryCount > policy.MaximumRedeliveries;
            }
            return false;
        }

        private bool IsMessageExpired(IMessageDelivery delivery)
        {
            Message.Message message = delivery.Message;
            if (message.NMSTimeToLive != TimeSpan.Zero)
            {
                DateTime now = DateTime.UtcNow;
                if (!IsBrowser && (message.NMSTimestamp + message.NMSTimeToLive) < now)
                {
                    return true;
                }
            }
            return false;
        }

        #endregion

        #region Protected Methods

        protected void EnterMessageListenerEvent()
        {
            try
            {
                if(!MessageListenerInUseEvent.SafeWaitHandle.IsClosed)
                    MessageListenerInUseEvent.Reset();
            }
            catch (Exception e)
            {
                Tracer.ErrorFormat("Failed to Reset MessageListener Event signal. Error : {0}", e);
            }
            
        }

        protected void LeaveMessageListenerEvent()
        {
            RemoveTaskRef();
            try
            {
                if (!MessageListenerInUseEvent.SafeWaitHandle.IsClosed)
                    MessageListenerInUseEvent.Set();
            }
            catch (Exception e)
            {
                Tracer.ErrorFormat("Failed to Send MessageListener Event signal. Error : {0}", e);
            }
        }

        protected bool WaitOnMessageListenerEvent(int timeout = -1)
        {
            bool signaled = false;
            if (OnMessage != null)
            {
                if (!MessageListenerInUseEvent.SafeWaitHandle.IsClosed)
                {
                    signaled = (timeout > -1) ? MessageListenerInUseEvent.WaitOne(timeout) : MessageListenerInUseEvent.WaitOne();
                }
                else if (!this.IsClosed)
                {
                    Tracer.WarnFormat("Failed to wait for Message Listener Event on consumer {0}", Id);
                }
            }
            
            return signaled;
        }

        protected void AddTaskRef()
        {
            System.Threading.Interlocked.Increment(ref pendingMessageDeliveryTasks);
            int lastPending = MaxPendingTasks;
            MaxPendingTasks = Math.Max(pendingMessageDeliveryTasks, MaxPendingTasks);
            if (lastPending < MaxPendingTasks)
            {
                //Tracer.WarnFormat("Consumer {0} Distpatch event highwatermark increased to {1}.", Id, MaxPendingTasks);
            }
        }

        protected void RemoveTaskRef()
        {
            System.Threading.Interlocked.Decrement(ref pendingMessageDeliveryTasks);
        }
        
        protected void OnInboundMessage(IReceiverLink link, Amqp.Message message)
        {
            Message.Message msg = null;
            try
            {
                IMessage nmsMessage = Message.AMQP.AMQPMessageBuilder.CreateProviderMessage(this, message);
                msg = nmsMessage as Message.Message;
                if(
                    Session.AcknowledgementMode.Equals(AcknowledgementMode.AutoAcknowledge) ||
                    Session.AcknowledgementMode.Equals(AcknowledgementMode.ClientAcknowledge)
                    )
                {
                    msg.GetMessageCloak().AckHandler = new Message.MessageAcknowledgementHandler(this, msg);
                }
            }
            catch (Exception e)
            {
                this.Session.OnException(e);
            }

            if(msg != null)
            {
                transportMsgCount++;
                
                SendForDelivery(new MessageDelivery(msg));
            }
        }

        protected void SendForDelivery(IMessageDelivery nmsDelivery)
        {
            this.messageQueue.Enqueue(nmsDelivery);

            if (this.OnMessage != null && pendingMessageDeliveryTasks < ExecutorThreshold)
            {
                AddTaskRef();
                DispatchEvent dispatchEvent = new MessageListenerDispatchEvent(this);
                Session.Dispatcher.Enqueue(dispatchEvent);
            }
            else if (pendingMessageDeliveryTasks < 0)
            {
                Tracer.ErrorFormat("Consumer {0} has invalid pending tasks count on executor {1}.", Id, pendingMessageDeliveryTasks);
            }
        }

        protected virtual void OnAttachResponse(ILink link, Attach attachResponse)
        {
            Tracer.InfoFormat("Received Performative Attach response on Link: {0}, Response: {1}", ConsumerId, attachResponse.ToString());
            OnResponse();
            if (link.Error != null)
            {
                this.Session.OnException(ExceptionSupport.GetException(link, "Consumer {0} Attach Failure.", this.ConsumerId));
            }
        }

        protected void SendFlow(int credit)
        {
            if (!mode.Value.Equals(Resource.Mode.Stopped))
            {
                this.Link.SetCredit(credit, false);
            }
        }

        protected virtual bool TryDequeue(out IMessageDelivery delivery, int timeout)
        {
            delivery = null;
            DateTime deadline = DateTime.UtcNow + TimeSpan.FromMilliseconds(timeout);
            Tracer.DebugFormat("Waiting for msg availability Deadline {0}", deadline);
            try
            {
                while (true)
                {
                    if(timeout == 0)
                    {
                        delivery = this.messageQueue.DequeueNoWait();
                    }
                    else
                    {
                        delivery = this.messageQueue.Dequeue(timeout);
                    }
                    
                    if (delivery == null)
                    {
                        if (timeout == 0 || this.Link.IsClosed || this.messageQueue.IsClosed)
                        {
                            return false;
                        }
                        else if (timeout > 0)
                        {
                            timeout = Math.Max((deadline - DateTime.UtcNow).Milliseconds, 0);
                        }
                    }
                    else if (IsMessageExpired(delivery))
                    {
                        DateTime now = DateTime.UtcNow;
                        Error err = new Error(NMSErrorCode.PROPERTY_ERROR);
                        err.Description = "Message Expired";
                        AckRejected(delivery,  err);
                        if (timeout > 0)
                        {
                            timeout = Math.Max((deadline - now).Milliseconds, 0);
                        }
                        if(Tracer.IsDebugEnabled)
                            Tracer.DebugFormat("{0} Filtered expired (deadline {1} Now {2}) message: {3}", ConsumerId, deadline, now, delivery);
                    }
                    else if (IsMessageRedeliveryExceeded(delivery))
                    {
                        if (Tracer.IsDebugEnabled)
                            Tracer.DebugFormat("{0} Filtered Message with excessive Redelivery Count: {1}", ConsumerId, delivery);
                        AckModified(delivery, true, true);
                        if (timeout > 0)
                        {
                            timeout = Math.Max((deadline - DateTime.UtcNow).Milliseconds, 0);
                        }
                    }
                    else
                    {
                        break;
                    }

                }
            }
            catch(Exception e)
            {
                throw ExceptionSupport.Wrap(e, "Failed to Received message on consumer {0}.", ConsumerId);
            }
            
            return true;
        }

        protected void ThrowIfAsync()
        {
            if (this.OnMessage != null)
            {
                throw new IllegalStateException("Cannot synchronously receive message on a synchronous consumer " + consumerInfo);
            }
        }

        protected void DrainMessageQueueIfAny()
        {
            if (OnMessage != null && messageQueue.Count > 0)
            {
                DispatchEvent deliveryTask = new MessageListenerDispatchEvent(this);
                Session.Dispatcher.Enqueue(deliveryTask);
            }
        }

        protected void PrepareMessageForDelivery(Message.Message message)
        {
            if (message == null) return;
            if(message is Message.BytesMessage)
            {
                (message as Message.BytesMessage).Reset();
            }
            else if(message is Message.StreamMessage)
            {
                (message as Message.StreamMessage).Reset();
            }
            else
            {
                message.IsReadOnly = true;
            }
            message.IsReadOnlyProperties = true;
        }

        #endregion

        #region Internal Methods

        internal bool HasSubscription(string name)
        {
            return !IsClosed && IsDurable && String.Compare(name, this.consumerInfo.SubscriptionName, false) == 0;
        }

        internal bool IsUsingDestination(IDestination destination)
        {
            return this.Destination.Equals(destination);
        }

        internal void Recover()
        {
            Tracer.DebugFormat("Session recover for consumer: {0}", Id);
            IMessageCloak cloak = null;
            IMessageDelivery delivery = null;
            lock (messageQueue.SyncRoot)
            {
                while ((delivery = delivered.Last?.Value) != null)
                {
                    cloak = delivery.Message.GetMessageCloak();
                    cloak.DeliveryCount = cloak.DeliveryCount + 1;
                    (delivery as MessageDelivery).EnqueueFirst = true;
                    delivered.RemoveLast();
                    SendForDelivery(delivery);
                }
                delivered.Clear();
            }
        }

        internal void AcknowledgeMessage(Message.Message message, Message.AckType ackType)
        {
            
            ThrowIfClosed();
            IMessageDelivery nmsDelivery = null;
            foreach (IMessageDelivery delivery in delivered)
            {
                if (delivery.Message.Equals(message))
                {
                    nmsDelivery = delivery;
                }
            }
            if(nmsDelivery == null)
            {
                nmsDelivery = new MessageDelivery(message);
            }
            switch (ackType)
            {
                case Message.AckType.ACCEPTED:
                    AckConsumed(nmsDelivery);
                    break;
                case Message.AckType.MODIFIED_FAILED:
                    AckModified(nmsDelivery, true);
                    break;
                case Message.AckType.MODIFIED_FAILED_UNDELIVERABLE:
                    AckModified(nmsDelivery, true, true);
                    break;
                case Message.AckType.REJECTED:
                    AckRejected(nmsDelivery);
                    break;
                case Message.AckType.RELEASED:
                    AckReleased(nmsDelivery);
                    break;
                default:
                    throw new NMSException("Unkown message acknowledgement type " + ackType);
            }
        }

        internal void Acknowledge(Message.AckType ackType)
        {
            
            foreach(IMessageDelivery delivery in delivered.ToArray())
            {
                switch (ackType)
                {
                    case Message.AckType.ACCEPTED:
                        AckConsumed(delivery);
                        break;
                    case Message.AckType.MODIFIED_FAILED:
                        AckModified(delivery, true);
                        break;
                    case Message.AckType.MODIFIED_FAILED_UNDELIVERABLE:
                        AckModified(delivery, true, true);
                        break;
                    case Message.AckType.REJECTED:
                        AckRejected(delivery);
                        break;
                    case Message.AckType.RELEASED:
                        AckReleased(delivery);
                        break;
                    default:
                        Tracer.WarnFormat("Unkown message acknowledgement type {0} for message {}", ackType, delivery.Message.NMSMessageId);
                        break;
                }
            }
            delivered.Clear();
        }

        #endregion

        #region IMessageConsumer Properties

        public ConsumerTransformerDelegate ConsumerTransformer
        {
            get
            {
                throw new NotImplementedException();
            }

            set
            {
                throw new NotImplementedException();
            }
        }

        #endregion

        #region IMessageConsumer Events
        protected event MessageListener OnMessage;

        public event MessageListener Listener
        {
            add
            {

                if (this.IsStarted)
                {
                    throw new IllegalStateException("Cannot add MessageListener to consumer " + Id + " on a started Connection.");
                }
                if(value != null)
                {
                    OnMessage += value;
                }
            }
            remove
            {
                if (this.IsStarted)
                {
                    throw new IllegalStateException("Cannot remove MessageListener to consumer " + Id + " on a started Connection.");
                }
                if (value != null)
                {
                    OnMessage -= value;
                }
            }
        }

        #endregion

        #region IMessageConsumer Methods
        
        public IMessage Receive()
        {
            ThrowIfClosed();
            ThrowIfAsync();
            if (TryDequeue(out IMessageDelivery delivery, -1))
            {
                Message.Message copy = delivery.Message.Copy();
                PrepareMessageForDelivery(copy);
                AckReceived(delivery);
                return copy;
            }
            return null;
        }

        public IMessage Receive(TimeSpan timeout)
        {
            ThrowIfClosed();
            ThrowIfAsync();
            int timeoutMilis = Convert.ToInt32(timeout.TotalMilliseconds);
            if(timeoutMilis == 0)
            {
                timeoutMilis = -1;
            }
            if (TryDequeue(out IMessageDelivery delivery, timeoutMilis))
            {
                Message.Message copy = delivery.Message.Copy();
                PrepareMessageForDelivery(copy);
                AckReceived(delivery);
                return copy;
            }
            return null;
        }

        public IMessage ReceiveNoWait()
        {
            ThrowIfClosed();
            ThrowIfAsync();
            if (TryDequeue(out IMessageDelivery delivery, 0))
            {
                Message.Message copy = delivery.Message.Copy();
                PrepareMessageForDelivery(copy);
                AckReceived(delivery);
                return copy;
            }
            return null;
        }

        #endregion

        #region MessageLink Methods

        private Target CreateTarget()
        {
            Target target = new Target();
            return target;
        }

        private Source CreateSource()
        {
            Source source = new Source();
            source.Address = UriUtil.GetAddress(Destination, this.Session.Connection);
            source.Outcomes = new Amqp.Types.Symbol[] 
            {
                SymbolUtil.ATTACH_OUTCOME_ACCEPTED,
                SymbolUtil.ATTACH_OUTCOME_RELEASED,
                SymbolUtil.ATTACH_OUTCOME_REJECTED,
                SymbolUtil.ATTACH_OUTCOME_MODIFIED
            };
            source.DefaultOutcome = MessageSupport.MODIFIED_FAILED_INSTANCE;

            if (this.IsDurable)
            {
                source.ExpiryPolicy = SymbolUtil.ATTACH_EXPIRY_POLICY_NEVER;
                source.Durable = (int)TerminusDurability.UNSETTLED_STATE;
                source.DistributionMode=SymbolUtil.ATTACH_DISTRIBUTION_MODE_COPY;
            }
            else
            {
                source.ExpiryPolicy = SymbolUtil.ATTACH_EXPIRY_POLICY_SESSION_END;
                source.Durable = (int)TerminusDurability.NONE;
            }
            
            if (this.IsBrowser)
            {
                source.DistributionMode = SymbolUtil.ATTACH_DISTRIBUTION_MODE_COPY;
            }

            source.Capabilities = new[] { SymbolUtil.GetTerminusCapabilitiesForDestination(Destination) };

            Amqp.Types.Map filters = new Amqp.Types.Map();

            // TODO add filters for noLocal and Selector using appropriate Amqp Described types

            // No Local
            // qpid jms defines a no local filter as an amqp described type 
            //      AmqpJmsNoLocalType where
            //          Descriptor = 0x0000468C00000003UL
            //          Described = "NoLocalFilter{}" (type string)
            if (consumerInfo.NoLocal)
            {
                filters.Add(SymbolUtil.ATTACH_FILTER_NO_LOCAL, "NoLocalFilter{}");
            }

            // Selector
            // qpid jms defines a selector filter as an amqp described type 
            //      AmqpJmsSelectorType where
            //          Descriptor = 0x0000468C00000004UL
            //          Described = "<selector_string>" (type string)
            if (this.HasSelector)
            {
                filters.Add(SymbolUtil.ATTACH_FILTER_SELECTOR, this.consumerInfo.Selector);
            }

            // Assign filters
            if (filters.Count > 0)
            {
                source.FilterSet = filters;
            }

            return source;
        }
        
        protected override ILink CreateLink()
        {
            Attach attach = new Amqp.Framing.Attach()
            {
                Target = CreateTarget(),
                Source = CreateSource(),
                RcvSettleMode = ReceiverSettleMode.First,
                SndSettleMode = (IsBrowser) ? SenderSettleMode.Settled : SenderSettleMode.Unsettled,
            };
            string name = null;
            if (IsDurable)
            {
                name = consumerInfo.SubscriptionName;
            }
            else
            {
                string destinationAddress = (attach.Source as Source).Address ?? "";
                name = "nms:receiver:" + this.ConsumerId.ToString() 
                    + ((destinationAddress.Length == 0) ? "" : (":" + destinationAddress));
            }
            IReceiverLink link = new ReceiverLink(Session.InnerSession as Amqp.Session, name, attach, OnAttachResponse);
            return link;
        }

        protected override void OnInternalClosed(IAmqpObject sender, Error error)
        {
            if (Tracer.IsDebugEnabled)
            {
                Tracer.DebugFormat("Received Close notification for MessageConsumer {0} {1} {2}",
                    this.Id,
                    IsDurable ? "with subscription name " + this.consumerInfo.SubscriptionName : "",
                    error == null ? "" : "with cause " + error);
            }
            base.OnInternalClosed(sender, error);
            this.OnResponse();
        }
        
        protected override void StopResource()
        {
            if (Session.Dispatcher.IsOnDispatchThread)
            {
                throw new IllegalStateException("Cannot stop Connection {0} in MessageListener.", Session.Connection.ClientId);
            }
            // Cut message window
            // TODO figure out draining message window without raising a closed window exception (link-credit-limit-exceeded Error) from amqpnetlite.
            //SendFlow(1);
            // Stop message delivery
            this.messageQueue.Stop();
            // Now wait until the MessageListener callback is finish executing.
            this.WaitOnMessageListenerEvent();
        }

        protected override void StartResource()
        {
            // Do Attach request if not done already
            base.StartResource();
            // Start Message Delivery
            messageQueue.Start();
            DrainMessageQueueIfAny();

            // Setup AMQP message transport thread callback
            OnInboundAMQPMessage = OnInboundMessage;
            // Open Message Window to receive messages.
            this.Link.Start(consumerInfo.LinkCredit, OnInboundAMQPMessage);
            
        }

        
        
        /// <summary>
        /// Executes the AMQP network detach operation.
        /// </summary>
        /// <param name="timeout">
        /// Timeout to wait for for detach response. A timeout of 0 or less will not block to wait for a response.
        /// </param>
        /// <param name="cause">Error to detach link. Can be null.</param>
        /// <exception cref="Amqp.AmqpException">
        /// Throws when an error occur during amqp detach. Or contains Error response from detach.
        /// </exception>
        /// <exception cref="System.TimeoutException">
        /// Throws when detach response is not received in specified timeout.
        /// </exception>
        protected override void DoClose(TimeSpan timeout, Error cause = null)
        {
            if(IsDurable)
            {
                Task t = this.Link.DetachAsync(cause);
                if(TimeSpan.Compare(timeout, TimeSpan.Zero) > 0)
                {
                    /*
                     * AmqpNetLite does not allow a timeout to be specific for link detach request even though
                     * it uses the same close operation which takes a parameter for timeout. AmqpNetLite uses 
                     * it default timeout of 60000ms, see AmqpObject.DefaultTimeout, for the detach close 
                     * operation forcing the detach request to be synchronous. To allow for asynchronous detach
                     * request an NMS MessageConsumer must call the DetachAsync method on a link which will block
                     * for up to 60000ms asynchronously to set a timeout exception or complete the task. 
                     */
                    const int amqpNetLiteDefaultTimeoutMillis = 60000; // taken from AmqpObject.DefaultTimeout
                    TimeSpan amqpNetLiteDefaultTimeout = TimeSpan.FromMilliseconds(amqpNetLiteDefaultTimeoutMillis);
                    // Create timeout which allows for the 60000ms block in the DetachAsync task.
                    TimeSpan actualTimeout = amqpNetLiteDefaultTimeout + timeout;
                    
                    TaskUtil.Wait(t, actualTimeout);
                    if(t.Exception != null)
                    {
                        if(t.Exception is AggregateException)
                        {
                            throw t.Exception.InnerException;
                        }
                        else
                        {
                            throw t.Exception;
                        }
                    }
                }
            }
            else
            {
                base.DoClose(timeout, cause);
            }
        }
        /// <summary>
        /// Overload for the Template method <see cref="MessageLink.Shutdown"/> specific to <see cref="MessageConsumer"/>.
        /// </summary>
        /// <param name="closeMessageQueue">Indicates whether or not to close the messageQueue for the MessageConsumer.</param>
        internal override void Shutdown()
        {
            this.messageQueue.Close();
        }

        #endregion

        #region IDisposable Methods
        public void Dispose()
        {
            try
            {
                this.Close();     
            }
            catch (Exception ex)
            {
                Tracer.DebugFormat("Caught exception while disposing {0} {1}. Exception {2}", this.GetType().Name, this.Id, ex);
            }   
        }
        protected override void Dispose(bool disposing)
        {
            if (!IsClosing && !IsClosed)
            {
                Tracer.InfoFormat("Consumer {0} stats: Transport Msgs {1}, Dispatch Msgs {2}, messageQueue {3}.",
                    Id, transportMsgCount, messageDispatchCount, messageQueue.Count);
            }
            base.Dispose(disposing);
            MessageListenerInUseEvent.Dispose();
        }


        #endregion

        #region Inner MessageListenerDispatchEvent Class

        protected class MessageListenerDispatchEvent : WaitableDispatchEvent 
        {
            private MessageConsumer consumer;
            
            internal MessageListenerDispatchEvent(MessageConsumer consumer) : base()
            {
                this.consumer = consumer;
                Callback = this.DispatchMessageListeners;
            }

            public override void OnFailure(Exception e)
            {
                base.OnFailure(e);
                consumer.Session.OnException(e);
            }

            public void DispatchMessageListeners()
            {
                IMessageDelivery delivery = null;
                Message.Message nmsProviderMessage = null;
                if (consumer.IsClosed) return;
                consumer.EnterMessageListenerEvent();
                // the consumer pending Message delivery task 
                
                while ((delivery = consumer.messageQueue.DequeueNoWait()) != null)
                {
                    nmsProviderMessage = delivery.Message;
                    consumer.AddTaskRef();
                    consumer.messageDispatchCount++;
                    try
                    {
                        
                        if (consumer.IsMessageExpired(delivery))
                        {
                            consumer.AckModified(delivery, true);
                        }
                        else if (consumer.IsMessageRedeliveryExceeded(delivery))
                        {
                            consumer.AckModified(delivery, true, true);
                        }
                        else
                        {
                            
                            bool deliveryFailed = false;
                            bool isAutoOrDupsOk = consumer.Session.AcknowledgementMode.Equals(AcknowledgementMode.AutoAcknowledge) ||
                                                  consumer.Session.AcknowledgementMode.Equals(AcknowledgementMode.DupsOkAcknowledge);
                            if (isAutoOrDupsOk)
                            {
                                consumer.delivered.AddLast(delivery);
                            }
                            else
                            {
                                consumer.AckReceived(delivery);
                            }
                             
                            Message.Message copy = nmsProviderMessage.Copy();
                            try
                            {
                                consumer.Session.ClearRecovered();
                                consumer.PrepareMessageForDelivery(copy);
                                if (Tracer.IsDebugEnabled)
                                    Tracer.DebugFormat("Invoking Client Message Listener Callback for message {0}.", copy.NMSMessageId);
                                consumer.OnMessage(copy);
                            }
                            catch (SystemException se)
                            {
                                Tracer.WarnFormat("Caught Exception on MessageListener for Consumer {0}. Message {1}.", consumer.Id, se.Message);
                                deliveryFailed = true;
                            }

                            if (isAutoOrDupsOk && !consumer.Session.IsRecovered)
                            {
                                if (!deliveryFailed)
                                {
                                    consumer.AckConsumed(delivery);
                                }
                                else
                                {
                                    consumer.AckReleased(delivery);
                                }
                            }
                        }

                    }
                    catch (Exception e)
                    {
                        // unhandled failure
                        consumer.Session.OnException(e);
                    }
                    consumer.RemoveTaskRef();
                }
                consumer.LeaveMessageListenerEvent();
            }
        }

        #endregion
    }

    #region Info class
    internal class ConsumerInfo : LinkInfo
    {
        
        protected const int DEFAULT_CREDIT = 200;

        private int? credit = null;

        internal ConsumerInfo(Id id) : base(id) { }

        public int LinkCredit
        {
            get { return credit ?? DEFAULT_CREDIT; }
            internal set { credit = value; }
        }

        public string Selector { get; internal set; } = null;
        public string SubscriptionName { get; internal set; } = null;

        public bool NoLocal { get; internal set; } = false;

    }

    #endregion

}
