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
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Provider.Amqp.Filters;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    public interface IAmqpConsumer
    {
        IDestination Destination { get; }
        IAmqpConnection Connection { get; }
    }

    public class AmqpConsumer : IAmqpConsumer
    {
        private readonly NmsConsumerInfo info;
        private ReceiverLink receiverLink;
        private readonly LinkedList<InboundMessageDispatch> messages;
        private readonly object syncRoot = new object();

        private readonly AmqpSession session;
        public IDestination Destination => info.Destination;
        public IAmqpConnection Connection => session.Connection;

        public AmqpConsumer(AmqpSession amqpSession, NmsConsumerInfo info)
        {
            session = amqpSession;
            this.info = info;

            messages = new LinkedList<InboundMessageDispatch>();
        }

        public NmsConsumerId ConsumerId => this.info.Id;

        public Task Attach()
        {
            Target target = new Target();
            Source source = CreateSource();

            Attach attach = new Attach
            {
                Target = target,
                Source = source,
                RcvSettleMode = ReceiverSettleMode.First,
                SndSettleMode = (info.IsBrowser) ? SenderSettleMode.Settled : SenderSettleMode.Unsettled,
            };
            string name;
            if (info.IsDurable)
            {
                name = info.SubscriptionName;
            }
            else
            {
                string destinationAddress = source.Address ?? "";
                name = "nms:receiver:" + info.Id
                                       + (destinationAddress.Length == 0 ? "" : (":" + destinationAddress));
            }

            // TODO: Add timeout
            var tsc = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            receiverLink = new ReceiverLink(session.UnderlyingSession, name, attach, HandleOpened(tsc));
            receiverLink.AddClosedCallback(HandleClosed(tsc));
            return tsc.Task;
        }

        private OnAttached HandleOpened(TaskCompletionSource<bool> tsc) => (link, attach) =>
        {
            if (IsClosePending(attach))
                return;

            tsc.SetResult(true);
        };

        private static bool IsClosePending(Attach attach)
        {
            // When no link terminus was created, the peer will now detach/close us otherwise
            // we need to validate the returned remote source prior to open completion.
            return attach.Source == null;
        }

        private ClosedCallback HandleClosed(TaskCompletionSource<bool> tsc) => (sender, error) =>
        {
            NMSException exception = ExceptionSupport.GetException(error, "Received Amqp link detach with Error for link {0}", info.Id);
            if (!tsc.TrySetException(exception))
            {
                session.RemoveConsumer(info.Id);

                // If session is not closed it means that the link was remotely detached 
                if (!receiverLink.Session.IsClosed)
                {
                    session.Connection.Provider.FireResourceClosed(info, exception);
                }
            }
        };

        private Source CreateSource()
        {
            Source source = new Source();
            source.Address = AmqpDestinationHelper.GetDestinationAddress(info.Destination, session.Connection);
            source.Outcomes = new[]
            {
                SymbolUtil.ATTACH_OUTCOME_ACCEPTED,
                SymbolUtil.ATTACH_OUTCOME_RELEASED,
                SymbolUtil.ATTACH_OUTCOME_REJECTED,
                SymbolUtil.ATTACH_OUTCOME_MODIFIED
            };
            source.DefaultOutcome = MessageSupport.MODIFIED_FAILED_INSTANCE;

            if (info.IsDurable)
            {
                source.ExpiryPolicy = SymbolUtil.ATTACH_EXPIRY_POLICY_NEVER;
                source.Durable = (int) TerminusDurability.UNSETTLED_STATE;
                source.DistributionMode = SymbolUtil.ATTACH_DISTRIBUTION_MODE_COPY;
            }
            else
            {
                source.ExpiryPolicy = SymbolUtil.ATTACH_EXPIRY_POLICY_SESSION_END;
                source.Durable = (int) TerminusDurability.NONE;
            }

            if (info.IsBrowser)
            {
                source.DistributionMode = SymbolUtil.ATTACH_DISTRIBUTION_MODE_COPY;
            }

            source.Capabilities = new[] { SymbolUtil.GetTerminusCapabilitiesForDestination(info.Destination) };

            Map filters = new Map();
            
            if (info.NoLocal)
            {
                filters.Add(SymbolUtil.ATTACH_FILTER_NO_LOCAL, AmqpNmsNoLocalType.NO_LOCAL);
            }

            if (info.HasSelector())
            {
                filters.Add(SymbolUtil.ATTACH_FILTER_SELECTOR, new AmqpNmsSelectorType(info.Selector));
            }

            if (filters.Count > 0)
            {
                source.FilterSet = filters;
            }

            return source;
        }

        public void Start()
        {
            receiverLink.Start(info.LinkCredit, OnMessage);
        }

        public void Stop()
        {
            receiverLink.SetCredit(0, CreditMode.Drain);
        }

        private void OnMessage(IReceiverLink receiver, global::Amqp.Message amqpMessage)
        {
            if (Tracer.IsDebugEnabled)
            {
                Tracer.Debug($"Received message {amqpMessage?.Properties?.MessageId}");
            }
            
            NmsMessage message;
            try
            {
                message = AmqpCodec.DecodeMessage(this, amqpMessage).AsMessage();
            }
            catch (Exception e)
            {
                Tracer.Error($"Error on transform: {e.Message}");

                // Mark message as undeliverable
                receiverLink.Modify(amqpMessage, true, true);
                return;
            }

            // Let the message do any final processing before sending it onto a consumer.
            // We could defer this to a later stage such as the NmsConnection or even in
            // the NmsMessageConsumer dispatch method if we needed to.
            message.OnDispatch();

            InboundMessageDispatch inboundMessageDispatch = new InboundMessageDispatch
            {
                Message = message,
                ConsumerId = info.Id,
                ConsumerInfo = info,
            };

            AddMessage(inboundMessageDispatch);

            IProviderListener providerListener = session.Connection.Provider.Listener;
            providerListener.OnInboundMessage(inboundMessageDispatch);
        }

        public void Acknowledge(AckType ackType)
        {
            foreach (InboundMessageDispatch envelope in GetMessages().Where(x => x.IsDelivered))
            {
                Acknowledge(envelope, ackType);
            }
        }

        public void Acknowledge(InboundMessageDispatch envelope, AckType ackType)
        {
            if (envelope.Message.Facade is AmqpNmsMessageFacade facade)
            {
                global::Amqp.Message message = facade.Message;
                switch (ackType)
                {
                    case AckType.DELIVERED:
                        envelope.IsDelivered = true;
                        break;
                    case AckType.ACCEPTED:
                        HandleAccepted(envelope, message);
                        break;
                    case AckType.RELEASED:
                        receiverLink.Release(message);
                        RemoveMessage(envelope);
                        break;
                    case AckType.MODIFIED_FAILED_UNDELIVERABLE:
                        receiverLink.Modify(message, true, true);
                        RemoveMessage(envelope);
                        break;
                    default:
                        Tracer.ErrorFormat("Unsupported Ack Type for message: {0}", envelope);
                        throw new ArgumentException($"Unsupported Ack Type for message: {envelope}");
                }
            }
            else
            {
                Tracer.ErrorFormat($"Received Ack for unknown message: {envelope}");
            }
        }

        private void HandleAccepted(InboundMessageDispatch envelope, global::Amqp.Message message)
        {
            Tracer.DebugFormat("Accepted Ack of message: {0}", envelope);
            
            if (session.IsTransacted)
            {
                if (!session.IsTransactionFailed)
                {
                    var transactionalState = session.TransactionContext;
                    receiverLink.Complete(message, transactionalState.GetTxnAcceptState());
                    transactionalState.RegisterTxConsumer(this);
                }
                else
                {
                    Tracer.DebugFormat("Skipping ack of message {0} in failed transaction.", envelope);
                }
                
            }
            else
            {
                receiverLink.Accept(message);
            }

            RemoveMessage(envelope);
        }

        private void AddMessage(InboundMessageDispatch envelope)
        {
            lock (syncRoot)
            {
                messages.AddLast(envelope);
            }
        }

        private void RemoveMessage(InboundMessageDispatch envelope)
        {
            lock (syncRoot)
            {
                messages.Remove(envelope);
            }
        }

        private InboundMessageDispatch[] GetMessages()
        {
            lock (syncRoot)
            {
                return messages.ToArray();
            }
        }

        public void Close()
        {
            if (info.IsDurable)
            {
                receiverLink?.Detach();
            }
            else
            {
                receiverLink?.Close();
            }
        }

        public void Recover()
        {
            Tracer.Debug($"Session Recover for consumer: {info.Id}");

            List<InboundMessageDispatch> redispatchList = new List<InboundMessageDispatch>();
            foreach (var envelope in GetMessages().Where(x => x.IsDelivered))
            {
                envelope.IsDelivered = false;
                envelope.Message.Facade.RedeliveryCount++;
                envelope.EnqueueFirst = true;
                redispatchList.Add(envelope);
            }

            IProviderListener providerListener = session.Connection.Provider.Listener;

            for (int i = redispatchList.Count - 1; i >= 0; i--)
            {
                providerListener.OnInboundMessage(redispatchList[i]);
            }
        }

        public bool HasSubscription(string subscriptionName)
        {
            return info.IsDurable && info.SubscriptionName.Equals(subscriptionName);
        }

        public void PostRollback()
        {
            var pendingMessages = GetMessages().Where(x => !x.IsDelivered);
            foreach (InboundMessageDispatch message in pendingMessages)
            {
                Acknowledge(message, AckType.RELEASED);
            }
        }
    }
}