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
using Amqp;
using Amqp.Framing;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    public class AmqpProducer
    {
        private static readonly OutcomeCallback _onOutcome = OnOutcome;
        
        private readonly AmqpSession session;
        private readonly NmsProducerInfo info;
        private SenderLink senderLink;

        public AmqpProducer(AmqpSession session, NmsProducerInfo info)
        {
            this.session = session;
            this.info = info;
        }

        public Task Attach()
        {
            Source source = CreateSource();
            Target target = CreateTarget();
            Attach frame = new Attach
            {
                Source = source,
                Target = target,
                SndSettleMode = SenderSettleMode.Unsettled,
                IncompleteUnsettled = false,
                InitialDeliveryCount = 0
            };

            string linkName = info.Id + ":" + target.Address;
            var taskCompletionSource = new TaskCompletionSource<bool>();
            senderLink = new SenderLink(session.UnderlyingSession, linkName, frame, HandleOpened(taskCompletionSource));

            senderLink.AddClosedCallback((sender, error) =>
            {
                NMSException exception = ExceptionSupport.GetException(error, "Received Amqp link detach with Error for link {0}", info.Id);
                if (!taskCompletionSource.TrySetException(exception))
                {
                    session.RemoveProducer(info.Id);

                    // If session is not closed it means that the link was remotely detached
                    if (!senderLink.Session.IsClosed)
                    {
                        session.Connection.Provider.FireResourceClosed(info, exception);
                    }
                }
            });

            return taskCompletionSource.Task;
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
            // we need to validate the returned remote target prior to open completion.
            return attach.Target == null;
        }

        private Source CreateSource() => new Source
        {
            Address = info.Id.ToString(),
            Outcomes = new[]
            {
                SymbolUtil.ATTACH_OUTCOME_ACCEPTED,
                SymbolUtil.ATTACH_OUTCOME_REJECTED,
            },
        };

        private Target CreateTarget()
        {
            Target target = new Target();
            target.Address = AmqpDestinationHelper.GetDestinationAddress(info.Destination, session.Connection);

            // Durable is used for a durable subscription
            target.Durable = (uint) TerminusDurability.NONE;

            if (info.Destination != null)
            {
                target.Capabilities = new[] { SymbolUtil.GetTerminusCapabilitiesForDestination(info.Destination) };
            }

            target.Dynamic = false;

            return target;
        }

        public async Task Send(OutboundMessageDispatch envelope)
        {
            if (envelope.Message.Facade is AmqpNmsMessageFacade facade)
            {
                AmqpCodec.EncodeMessage(facade);

                global::Amqp.Message message = facade.Message;

                try
                {
                    // If the transaction has failed due to remote termination etc then we just indicate
                    // the send has succeeded until the a new transaction is started.
                    if (session.IsTransacted && session.IsTransactionFailed)
                        return;

                    var transactionalState = session.TransactionContext?.GetTxnEnrolledState();

                    if (envelope.FireAndForget)
                    {
                        SendSync(message, transactionalState);
                        return;
                    }
                    await SendAsync(message, transactionalState).ConfigureAwait(false);
                }
                catch (AmqpException amqpEx)
                {
                    string messageId = AmqpMessageIdHelper.ToMessageIdString(message.Properties?.GetMessageId());
                    throw ExceptionSupport.Wrap(amqpEx, $"Failure to send message {messageId} on Producer {info.Id}");
                }
                catch (Exception ex)
                {
                    Tracer.ErrorFormat(
                        $"Encountered Error on sending message from Producer {info.Id}. Message: {ex.Message}. Stack : {ex.StackTrace}.");
                    throw ExceptionSupport.Wrap(ex);
                }
            }
        }

        private void SendSync(global::Amqp.Message message, DeliveryState deliveryState)
        {
            senderLink.Send(message, deliveryState, null, null);
        }
        
        private async Task SendAsync(global::Amqp.Message message, DeliveryState deliveryState)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            CancellationTokenSource cts = null;
            if (session.Connection.Provider.SendTimeout != NmsConnectionInfo.INFINITE)
            {
                cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(session.Connection.Provider.SendTimeout));
                cts.Token.Register(_ =>
                {
                    var timeoutException = ExceptionSupport.GetTimeoutException(this.senderLink, $"The operation did not complete within the allocated time {session.Connection.Provider.SendTimeout}ms.");
                    tcs.TrySetException(timeoutException);
                }, null);
            }
            try
            {
                senderLink.Send(message, deliveryState, _onOutcome, tcs);
                await tcs.Task.ConfigureAwait(false);
            }
            finally
            {
                cts?.Dispose();
            }
        }
        
        private static void OnOutcome(ILink sender, global::Amqp.Message message, Outcome outcome, object state)
        {
            var tcs = (TaskCompletionSource<bool>) state;
            if (outcome.Descriptor.Code == MessageSupport.ACCEPTED_INSTANCE.Descriptor.Code)
            {
                tcs.TrySetResult(true);
            }
            else if (outcome.Descriptor.Code == MessageSupport.REJECTED_INSTANCE.Descriptor.Code)
            {
                Rejected rejected = (Rejected) outcome;
                tcs.TrySetException(ExceptionSupport.GetException(rejected.Error, $"Message {message.Properties.GetMessageId()} rejected"));
            }
            else if (outcome.Descriptor.Code == MessageSupport.RELEASED_INSTANCE.Descriptor.Code)
            {
                Error error = new Error(ErrorCode.MessageReleased);
                tcs.TrySetException(ExceptionSupport.GetException(error, $"Message {message.Properties.GetMessageId()} released"));
            }
            else
            {
                Error error = new Error(ErrorCode.InternalError);
                tcs.TrySetException(ExceptionSupport.GetException(error, outcome.ToString()));
            }
        }

        public void Close()
        {
            try
            {
                var closeTimeout = session.Connection.Provider.CloseTimeout;
                senderLink.Close(TimeSpan.FromMilliseconds(closeTimeout));
            }
            catch (NMSException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw ExceptionSupport.Wrap(ex, "Failed to close Link {0}", this.info.Id);
            }
        }
    }
}