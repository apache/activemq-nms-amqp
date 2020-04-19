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
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using Amqp.Transactions;
using Amqp.Types;
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Util;
using NLog;
using NMS.AMQP.Test.TestAmqp.BasicTypes;
using NMS.AMQP.Test.TestAmqp.Matchers;
using NUnit.Framework;

namespace NMS.AMQP.Test.TestAmqp
{
    public class TestAmqpPeer : IDisposable
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();
        
        public static readonly string MESSAGE_NUMBER = "MessageNumber";

        private static readonly Symbol ANONYMOUS = new Symbol("ANONYMOUS");
        private static readonly Symbol PLAIN = new Symbol("PLAIN");

        private static readonly Symbol[] DEFAULT_DESIRED_CAPABILITIES =
        {
            SymbolUtil.OPEN_CAPABILITY_SOLE_CONNECTION_FOR_CONTAINER,
            SymbolUtil.OPEN_CAPABILITY_DELAYED_DELIVERY,
            SymbolUtil.OPEN_CAPABILITY_ANONYMOUS_RELAY,
            SymbolUtil.OPEN_CAPABILITY_SHARED_SUBS
        };

        private const int CONNECTION_CHANNEL = 0;

        private ushort lastInitiatedChannel = 0;
        private uint lastInitiatedLinkHandle;
        private uint lastInitiatedCoordinatorLinkHandle = 0;

        private readonly LinkedList<IMatcher> matchers = new LinkedList<IMatcher>();
        private readonly object matchersLock = new object();
        private readonly TestAmqpPeerRunner testAmqpPeerRunner;
        private CountdownEvent matchersCompletedLatch;

        public TestAmqpPeer()
        {
            testAmqpPeerRunner = new TestAmqpPeerRunner(this, new IPEndPoint(IPAddress.Any, 0));
            Open();
        }

        public int ServerPort => testAmqpPeerRunner.Port;
        public Socket ClientSocket => testAmqpPeerRunner?.ClientSocket;

        public void Dispose()
        {
            Close();
        }

        private void Open()
        {
            testAmqpPeerRunner.Open();
        }

        public void OnHeader(Stream stream, byte[] header)
        {
            var matcher = GetFirstMatcher();
            if (matcher is HeaderMatcher headerMatcher)
            {
                RemoveFirstMatcher();
                headerMatcher.OnHeader(stream, header);
            }
            else
            {
                stream.Write(header, 0, header.Length);
            }
        }

        public bool OnFrame(Stream stream, ushort channel, DescribedList command, Amqp.Message message)
        {
            Logger.Debug($"Received frame, descriptor={command.GetType().Name} on port: {ServerPort}");
            
            var matcher = GetFirstMatcher();
            if (matcher != null)
            {
                if (matcher is IFrameMatcher frameMatcher)
                {
                    RemoveFirstMatcher();
                    return frameMatcher.OnFrame(stream, channel, command, message);
                }

                Assert.Fail($"Received frame but the next matcher is a {matcher.GetType().Name}");
            }
            else
            {
                Assert.Fail($"No matcher! Received frame, descriptor={command.GetType().Name}");
            }

            return false;
        }

        public void ExpectSaslPlain(string username, string password)
        {
            var b1 = Encoding.UTF8.GetBytes(username);
            var b2 = Encoding.UTF8.GetBytes(password);
            var message = new byte[2 + b1.Length + b2.Length];
            Array.Copy(b1, 0, message, 1, b1.Length);
            Array.Copy(b2, 0, message, b1.Length + 2, b2.Length);

            Action<byte[]> initialResponseMatcher = initialResponse => CollectionAssert.AreEqual(message, initialResponse);

            ExpectSaslAuthentication(mechanism: PLAIN, initialResponseMatcher: initialResponseMatcher);
        }

        public void ExpectSaslAnonymous()
        {
            var message = Encoding.UTF8.GetBytes(ANONYMOUS.ToString());

            Action<byte[]> initialResponseMatcher = initialResponse => CollectionAssert.AreEqual(message, initialResponse);
            ExpectSaslAuthentication(mechanism: ANONYMOUS, initialResponseMatcher: initialResponseMatcher);
        }

        private void ExpectSaslAuthentication(Symbol mechanism, Action<byte[]> initialResponseMatcher)
        {
            var headerMatcher = new HeaderMatcher(AmqpHeader.SASL_HEADER, AmqpHeader.SASL_HEADER)
                .WithOnComplete(context => context.SendCommand(new SaslMechanisms { SaslServerMechanisms = new[] { mechanism } }));

            AddMatcher(headerMatcher);

            var saslInitMatcher = new FrameMatcher<SaslInit>()
                .WithAssertion(saslInit => Assert.AreEqual(mechanism, saslInit.Mechanism))
                .WithAssertion(saslInit => initialResponseMatcher(saslInit.InitialResponse))
                .WithOnComplete(context => { context.SendCommand(new SaslOutcome { Code = SaslCode.Ok }, FrameType.Sasl); })
                .WithShouldContinue(false);

            AddMatcher(saslInitMatcher);
        }

        public void ExpectOpen(Fields serverProperties = null)
        {
            ExpectOpen(desiredCapabilities: DEFAULT_DESIRED_CAPABILITIES, serverCapabilities: new[] { SymbolUtil.OPEN_CAPABILITY_SOLE_CONNECTION_FOR_CONTAINER }, serverProperties: serverProperties);
        }

        public void ExpectOpen(Symbol[] serverCapabilities, Fields serverProperties)
        {
            ExpectOpen(desiredCapabilities: DEFAULT_DESIRED_CAPABILITIES, serverCapabilities: serverCapabilities, serverProperties: serverProperties);
        }

        private void ExpectOpen(Symbol[] desiredCapabilities, Symbol[] serverCapabilities, Fields serverProperties)
        {
            var openMatcher = new FrameMatcher<Open>();

            if (desiredCapabilities != null)
                openMatcher.WithAssertion(open => CollectionAssert.AreEquivalent(desiredCapabilities, open.DesiredCapabilities));
            else
                openMatcher.WithAssertion(open => Assert.IsNull(open.DesiredCapabilities));

            openMatcher.WithOnComplete(context =>
            {
                var open = new Open
                {
                    ContainerId = Guid.NewGuid().ToString(),
                    OfferedCapabilities = serverCapabilities,
                    Properties = serverProperties
                };
                context.SendCommand(open);
            });

            AddMatcher(openMatcher);
        }

        public void RejectConnect(Symbol errorType, string errorMessage)
        {
            // Expect a connection, establish through the SASL negotiation and sending of the Open frame
            Fields serverProperties = new Fields { { SymbolUtil.CONNECTION_ESTABLISH_FAILED, SymbolUtil.BOOLEAN_TRUE } };
            ExpectSaslAnonymous();
            ExpectOpen(serverProperties: serverProperties);

            // Now generate the Close frame with the supplied error and update the matcher to send the Close frame after the Open frame.
            IMatcher lastMatcher = GetLastMatcher();
            lastMatcher.WithOnComplete(context =>
            {
                var close = new Close { Error = new Error(errorType) { Description = errorMessage} };
                context.SendCommand(CONNECTION_CHANNEL, close);
            });

            var closeMatcher = new FrameMatcher<Close>()
                .WithAssertion(close => Assert.IsNull(close.Error));

            AddMatcher(closeMatcher);
        }

        public void ExpectBegin(int nextOutgoingId = 1, bool sendResponse = true)
        {
            var frameMatcher = new FrameMatcher<Begin>()
                .WithAssertion(begin => Assert.AreEqual(nextOutgoingId, begin.NextOutgoingId))
                .WithAssertion(begin => Assert.NotNull(begin.IncomingWindow));

            if (sendResponse)
                frameMatcher.WithOnComplete(context =>
                {
                    var begin = new Begin
                    {
                        RemoteChannel = context.Channel,
                        NextOutgoingId = 1,
                        IncomingWindow = 0,
                        OutgoingWindow = 0
                    };
                    context.SendCommand(begin);
                    lastInitiatedChannel = context.Channel;
                });

            AddMatcher(frameMatcher);
        }

        public void ExpectEnd(bool sendResponse = true)
        {
            var endMatcher = new FrameMatcher<End>();

            if (sendResponse)
            {
                endMatcher.WithOnComplete(context => context.SendCommand(new End()));
            }

            AddMatcher(endMatcher);
        }

        public void ExpectClose()
        {
            var closeMatcher = new FrameMatcher<Close>()
                .WithAssertion(close => Assert.IsNull(close.Error))
                .WithOnComplete(context => context.SendCommand(new Close()));

            AddMatcher(closeMatcher);
        }

        public void ExpectLinkFlow(bool drain = false)
        {
            Action<uint> creditMatcher = credit => Assert.Greater(credit, 0);

            ExpectLinkFlow(drain: drain, sendDrainFlowResponse: false, creditMatcher: creditMatcher);
        }

        public void ExpectLinkFlow(bool drain, bool sendDrainFlowResponse, Action<uint> creditMatcher = null)
        {
            ExpectLinkFlowRespondWithTransfer(
                message: null,
                count: 0,
                drain: drain,
                sendDrainFlowResponse: sendDrainFlowResponse,
                sendSettled: false,
                addMessageNumberProperty: false,
                creditMatcher: creditMatcher,
                nextIncomingId: null
            );
        }

        public void ExpectLinkFlowRespondWithTransfer(Amqp.Message message, int count = 1, bool addMessageNumberProperty = false)
        {
            Action<uint> creditMatcher = credit => Assert.Greater(credit, 0);

            ExpectLinkFlowRespondWithTransfer(
                message: message,
                count: count,
                drain: false,
                sendDrainFlowResponse: false,
                sendSettled: false,
                addMessageNumberProperty: addMessageNumberProperty,
                creditMatcher: creditMatcher,
                nextIncomingId: 1
            );
        }

        public void ExpectLinkFlowRespondWithTransfer(
            Amqp.Message message,
            int count,
            bool drain,
            bool sendDrainFlowResponse,
            bool sendSettled,
            bool addMessageNumberProperty,
            Action<uint> creditMatcher,
            int? nextIncomingId)
        {
            if (nextIncomingId == null && count > 0)
            {
                Assert.Fail("The remote NextIncomingId must be specified if transfers have been requested");
            }

            FrameMatcher<Flow> flowMatcher = new FrameMatcher<Flow>()
                .WithAssertion(flow => Assert.AreEqual(drain, flow.Drain))
                .WithAssertion(flow => creditMatcher?.Invoke(flow.LinkCredit));

            if (nextIncomingId != null)
            {
                flowMatcher.WithAssertion(flow => Assert.AreEqual(nextIncomingId.Value, flow.NextIncomingId));
            }
            else
            {
                flowMatcher.WithAssertion(flow => Assert.GreaterOrEqual(flow.NextIncomingId, 0));
            }

            for (int i = 0; i < count; i++)
            {
                int nextId = nextIncomingId + i ?? i;
                byte[] deliveryTag = Encoding.UTF8.GetBytes("theDeliveryTag" + nextId);

                if (addMessageNumberProperty)
                {
                    if (message.ApplicationProperties == null)
                        message.ApplicationProperties = new ApplicationProperties();

                    message.ApplicationProperties[MESSAGE_NUMBER] = i;
                }

                if (message.Properties == null)
                    message.Properties = new Properties();
                
                message.Properties.MessageId = $"ID:{i.ToString()}";

                var messageId = message.Properties.MessageId;

                ByteBuffer payload = message.Encode();

                flowMatcher.WithOnComplete(context =>
                {
                    Logger.Debug($"Sending message {messageId}");

                    var transfer = new Transfer()
                    {
                        DeliveryId = (uint) nextId,
                        DeliveryTag = deliveryTag,
                        MessageFormat = 0,
                        Settled = sendSettled,
                        Handle = context.Command.Handle,
                    };

                    try
                    {
                        context.SendCommand(transfer, payload);
                    }
                    catch (Exception e)
                    {
                        Logger.Error($"Sending message {messageId} failed.");
                        throw;
                    }
                });
            }

            if (drain && sendDrainFlowResponse)
            {
                flowMatcher.WithOnComplete(context =>
                {
                    var flow = new Flow()
                    {
                        OutgoingWindow = 0,
                        IncomingWindow = uint.MaxValue,
                        LinkCredit = 0,
                        Drain = true,
                        Handle = context.Command.Handle,
                        DeliveryCount = context.Command.DeliveryCount + context.Command.LinkCredit,
                        NextOutgoingId = context.Command.NextIncomingId + (uint) count,
                        NextIncomingId = context.Command.NextOutgoingId
                    };

                    context.SendCommand(flow);
                });
            }

            AddMatcher(flowMatcher);
        }
        
        public void SendTransferToLastOpenedLinkOnLastOpenedSession(Amqp.Message message, uint nextIncomingId)
        {
            lock (matchersLock)
            {
                var lastMatcher = GetLastMatcher();
                ByteBuffer payload = message.Encode();
                lastMatcher.WithOnComplete(context =>
                {
                    var transfer = new Transfer()
                    {
                        DeliveryId = nextIncomingId,
                        DeliveryTag = Encoding.UTF8.GetBytes("theDeliveryTag" + nextIncomingId),
                        MessageFormat = 0,
                        Settled = false,
                        Handle = lastInitiatedLinkHandle,
                    };

                    context.SendCommand(transfer, payload);
                });
            }
        }

        public void ExpectSenderAttachWithoutGrantingCredit()
        {
            ExpectSenderAttach(sourceMatcher: Assert.NotNull, targetMatcher: Assert.NotNull, creditAmount: 0);
        }

        public void ExpectSenderAttach()
        {
            ExpectSenderAttach(sourceMatcher: Assert.NotNull, targetMatcher: Assert.NotNull);
        }

        public void ExpectSenderAttach(Action<object> sourceMatcher,
            Action<object> targetMatcher,
            bool refuseLink = false,
            uint creditAmount = 100,
            bool senderSettled = false,
            Error error = null)
        {
            var attachMatcher = new FrameMatcher<Attach>()
                .WithAssertion(attach => Assert.IsNotNull(attach.LinkName))
                .WithAssertion(attach => Assert.AreEqual(attach.Role, Role.SENDER))
                .WithAssertion(attach => Assert.AreEqual(senderSettled ? SenderSettleMode.Settled : SenderSettleMode.Unsettled, attach.SndSettleMode))
                .WithAssertion(attach => Assert.AreEqual(attach.RcvSettleMode, ReceiverSettleMode.First))
                .WithAssertion(attach => sourceMatcher(attach.Source))
                .WithAssertion(attach => targetMatcher(attach.Target))
                .WithOnComplete(context =>
                {
                    var attach = new Attach()
                    {
                        Role = Role.RECEIVER,
                        OfferedCapabilities = null,
                        SndSettleMode = senderSettled ? SenderSettleMode.Settled : SenderSettleMode.Unsettled,
                        RcvSettleMode = ReceiverSettleMode.First,
                        Handle = context.Command.Handle,
                        LinkName = context.Command.LinkName,
                        Source = context.Command.Source
                    };

                    if (refuseLink)
                        attach.Target = null;
                    else
                        attach.Target = context.Command.Target;

                    lastInitiatedLinkHandle = context.Command.Handle;

                    if (context.Command.Target is Coordinator)
                    {
                        lastInitiatedCoordinatorLinkHandle = context.Command.Handle;
                    }

                    context.SendCommand(attach);

                    if (refuseLink)
                    {
                        var detach = new Detach { Closed = true, Handle = context.Command.Handle };
                        if (error != null)
                        {
                            detach.Error = error;
                        }

                        context.SendCommand(detach);
                    }
                    else
                    {
                        var flow = new Flow
                    {
                        NextIncomingId = 1,
                        IncomingWindow = 2048,
                        NextOutgoingId = 1,
                        OutgoingWindow = 2024,
                        LinkCredit = creditAmount,
                        Handle = context.Command.Handle,
                        DeliveryCount = context.Command.InitialDeliveryCount,
                    };

                    context.SendCommand(flow);
                    }
                });

            AddMatcher(attachMatcher);
        }

        public void RemotelyCloseConnection(bool expectCloseResponse, Symbol errorCondition = null, string errorMessage = null)
        {
            lock (matchersLock)
            {
                var matcher = GetLastMatcher();
                matcher.WithOnComplete(context =>
                {
                    var close = new Close
                    {
                        Error = new Error(errorCondition) { Description = errorMessage }
                    };
                    context.SendCommand(close);
                });

                if (expectCloseResponse)
                {
                    // Expect a response to our Close.
                    var closeMatcher = new FrameMatcher<Close>();
                    AddMatcher(closeMatcher);
                }
            }
        }

        public void ExpectReceiverAttach()
        {
            Action<string> linkNameMatcher = Assert.IsNotNull;
            Action<Source> sourceMatcher = Assert.IsNotNull;
            Action<Target> targetMatcher = Assert.IsNotNull;

            ExpectReceiverAttach(linkNameMatcher: linkNameMatcher, sourceMatcher: sourceMatcher, targetMatcher: targetMatcher);
        }

        public void ExpectReceiverAttach(
            Action<string> linkNameMatcher,
            Action<Source> sourceMatcher,
            Action<Target> targetMatcher,
            bool settled = false,
            bool refuseLink = false,
            Symbol errorType = null,
            string errorMessage = null,
            Source responseSourceOverride = null,
            Action<Symbol[]> desiredCapabilitiesMatcher = null)
        {
            var attachMatcher = new FrameMatcher<Attach>()
                .WithAssertion(attach => linkNameMatcher(attach.LinkName))
                .WithAssertion(attach => Assert.AreEqual(Role.RECEIVER, attach.Role))
                .WithAssertion(attach => Assert.AreEqual(settled ? SenderSettleMode.Settled : SenderSettleMode.Unsettled, attach.SndSettleMode))
                .WithAssertion(attach => Assert.AreEqual(ReceiverSettleMode.First, attach.RcvSettleMode))
                .WithAssertion(attach => sourceMatcher(attach.Source as Source))
                .WithAssertion(attach => targetMatcher(attach.Target as Target))
                .WithOnComplete(context =>
                {
                    var attach = new Attach()
                    {
                        Role = Role.SENDER,
                        SndSettleMode = SenderSettleMode.Unsettled,
                        RcvSettleMode = ReceiverSettleMode.First,
                        InitialDeliveryCount = 0,
                        Handle = context.Command.Handle,
                        LinkName = context.Command.LinkName,
                        Target = context.Command.Target,
                    };

                    if (refuseLink)
                        attach.Source = null;
                    else if (responseSourceOverride != null)
                        attach.Source = responseSourceOverride;
                    else
                        attach.Source = context.Command.Source;

                    this.lastInitiatedLinkHandle = context.Command.Handle;

                    context.SendCommand(attach);
                });

            if (desiredCapabilitiesMatcher != null)
            {
                attachMatcher.WithAssertion(attach => desiredCapabilitiesMatcher(attach.DesiredCapabilities));
            }

            if (refuseLink)
            {
                attachMatcher.WithOnComplete(context =>
                {
                    var detach = new Detach { Closed = true, Handle = context.Command.Handle };
                    context.SendCommand(detach);
                });
            }

            AddMatcher(attachMatcher);
        }

        public void ExpectDurableSubscriberAttach(string topicName, string subscriptionName)
        {
            Action<string> linkNameMatcher = linkName => Assert.AreEqual(subscriptionName, linkName);

            Action<object> sourceMatcher = o =>
            {
                Assert.IsInstanceOf<Source>(o);
                var source = (Source) o;
                Assert.AreEqual(topicName, source.Address);
                Assert.IsFalse(source.Dynamic);
                Assert.AreEqual(TerminusDurability.UNSETTLED_STATE, (TerminusDurability) source.Durable);
                Assert.AreEqual(TerminusExpiryPolicy.NEVER, source.ExpiryPolicy);
                CollectionAssert.Contains(source.Capabilities, SymbolUtil.ATTACH_CAPABILITIES_TOPIC);
            };

            Action<Target> targetMatcher = Assert.IsNotNull;

            ExpectReceiverAttach(linkNameMatcher: linkNameMatcher, sourceMatcher: sourceMatcher, targetMatcher: targetMatcher, settled: false, errorType: null, errorMessage: null);
        }

        public void ExpectDetach(bool expectClosed, bool sendResponse, bool replyClosed, Symbol errorType = null, String errorMessage = null)
        {
            var detachMatcher = new FrameMatcher<Detach>()
                .WithAssertion(detach => Assert.AreEqual(expectClosed, detach.Closed));

            if (sendResponse)
            {
                detachMatcher.WithOnComplete(context =>
                {
                    var detach = new Detach
                    {
                        Closed = replyClosed,
                        Handle = context.Command.Handle
                    };

                    if (errorType != null)
                    {
                        detach.Error = new Error(errorType) { Description = errorMessage };
                    }

                    context.SendCommand(detach);
                });
            }

            AddMatcher(detachMatcher);
        }

        public void RemotelyDetachLastOpenedLinkOnLastOpenedSession(bool expectDetachResponse, bool closed, Symbol errorType, string errorMessage, int delayBeforeSend = 0)
        {
            lock (matchers)
            {
                IMatcher lastMatcher = GetLastMatcher();
                lastMatcher.WithOnComplete(context =>
                {
                    Detach detach = new Detach() { Closed = closed, Handle = lastInitiatedLinkHandle };
                    if (errorType != null)
                    {
                        detach.Error = new Error(errorType)
                        {
                            Description = errorMessage,
                        };
                    }

                    //Insert a delay if requested
                    if (delayBeforeSend > 0)
                    {
                        Thread.Sleep(delayBeforeSend);
                    }

                    context.SendCommand(lastInitiatedChannel, detach);
                });

                if (expectDetachResponse)
                {
                    // Expect a response to our Detach.
                    var detachMatcher = new FrameMatcher<Detach>()
                        .WithAssertion(detach => Assert.AreEqual(closed, detach.Closed));

                    // TODO: enable matching on the channel number of the response.
                    AddMatcher(detachMatcher);
                }
            }
        }

        public void ExpectDispositionThatIsAcceptedAndSettled()
        {
            Action<DeliveryState> stateMatcher = state => { Assert.AreEqual(state.Descriptor.Code, MessageSupport.ACCEPTED_INSTANCE.Descriptor.Code); };

            ExpectDisposition(settled: true, stateMatcher: stateMatcher);
        }

        public void ExpectDispositionThatIsReleasedAndSettled()
        {
            Action<DeliveryState> stateMatcher = state => { Assert.AreEqual(state.Descriptor.Code, MessageSupport.RELEASED_INSTANCE.Descriptor.Code); };

            ExpectDisposition(settled: true, stateMatcher: stateMatcher);
        }

        public void ExpectDisposition(bool settled, Action<DeliveryState> stateMatcher, uint? firstDeliveryId = null, uint? lastDeliveryId = null)
        {
            var dispositionMatcher = new FrameMatcher<Dispose>()
                .WithAssertion(dispose => Assert.AreEqual(settled, dispose.Settled))
                .WithAssertion(dispose => stateMatcher(dispose.State));

            if (firstDeliveryId.HasValue)
                dispositionMatcher.WithAssertion(dispose => Assert.AreEqual(firstDeliveryId.Value, dispose.First));

            if (lastDeliveryId.HasValue)
                dispositionMatcher.WithAssertion(dispose => Assert.AreEqual(lastDeliveryId.Value, dispose.Last));

            AddMatcher(dispositionMatcher);
        }

        public void ExpectSettledTransactionalDisposition(byte[] txnId)
        {
            void StateMatcher(DeliveryState state)
            {
                Assert.IsInstanceOf<TransactionalState>(state);
                var transactionalState = (TransactionalState) state;
                Assert.IsInstanceOf<Accepted>(transactionalState.Outcome);
                CollectionAssert.AreEqual(txnId, transactionalState.TxnId);
            }

            ExpectDisposition(settled: true, StateMatcher);
        }

        public void ExpectTransferButDoNotRespond(Action<Amqp.Message> messageMatcher)
        {
            ExpectTransfer(messageMatcher: messageMatcher,
                stateMatcher: Assert.IsNull,
                settled: false,
                sendResponseDisposition: false,
                responseState: null,
                responseSettled: false);
        }

        public void ExpectTransfer(Action<Amqp.Message> messageMatcher)
        {
            ExpectTransfer(messageMatcher: messageMatcher,
                stateMatcher: Assert.IsNull,
                settled: false,
                sendResponseDisposition: true,
                responseState: new Accepted(),
                responseSettled: true
            );
        }

        public void ExpectTransfer(Action<Amqp.Message> messageMatcher, Action<DeliveryState> stateMatcher, DeliveryState responseState, bool responseSettled)
        {
            ExpectTransfer(messageMatcher: messageMatcher,
                stateMatcher: stateMatcher,
                settled: false,
                sendResponseDisposition: true,
                responseState: responseState,
                responseSettled: responseSettled
            );
        }

        public void ExpectTransfer(Action<Amqp.Message> messageMatcher,
            Action<DeliveryState> stateMatcher,
            bool settled,
            bool sendResponseDisposition,
            DeliveryState responseState,
            bool responseSettled,
            int dispositionDelay = 0,
            bool batchable = false
        )
        {
            var transferMatcher = new FrameMatcher<Transfer>()
                .WithAssertion(transfer => Assert.AreEqual(settled, transfer.Settled))
                .WithAssertion(transfer => stateMatcher(transfer.State))
                .WithAssertion(transfer => Assert.AreEqual(batchable, transfer.Batchable))
                .WithAssertion(messageMatcher);

            if (sendResponseDisposition)
            {
                transferMatcher.WithOnComplete(context =>
                {
                    if (dispositionDelay > 0)
                    {
                        Thread.Sleep(dispositionDelay);
                    }
                    
                    var dispose = new Dispose()
                    {
                        Role = Role.RECEIVER,
                        Settled = responseSettled,
                        State = responseState,
                        First = context.Command.DeliveryId,
                    };
                    context.SendCommand(dispose);
                });
            }

            AddMatcher(transferMatcher);
        }

        public void ExpectTempTopicCreationAttach(string dynamicAddress)
        {
            ExpectTempNodeCreationAttach(dynamicAddress: dynamicAddress, nodeTypeCapability: SymbolUtil.ATTACH_CAPABILITIES_TEMP_TOPIC, sendResponse: true);
        }

        public void ExpectTempQueueCreationAttach(string dynamicAddress)
        {
            ExpectTempNodeCreationAttach(dynamicAddress: dynamicAddress, nodeTypeCapability: SymbolUtil.ATTACH_CAPABILITIES_TEMP_QUEUE, sendResponse: true);
        }

        public void ExpectTempNodeCreationAttach(string dynamicAddress, Symbol nodeTypeCapability, bool sendResponse)
        {
            Action<Target> targetMatcher = target =>
            {
                Assert.NotNull(target);
                Assert.IsNull(target.Address);
                Assert.IsTrue(target.Dynamic);
                Assert.AreEqual((uint) TerminusDurability.NONE, target.Durable);
                Assert.AreEqual(TerminusExpiryPolicy.LINK_DETACH, target.ExpiryPolicy);
                Assert.IsTrue(target.DynamicNodeProperties.ContainsKey(SymbolUtil.ATTACH_DYNAMIC_NODE_PROPERTY_LIFETIME_POLICY));
                CollectionAssert.Contains(target.Capabilities, nodeTypeCapability);
            };

            var attachMatcher = new FrameMatcher<Attach>()
                .WithAssertion(attach => Assert.NotNull(attach.LinkName))
                .WithAssertion(attach => Assert.AreEqual(Role.SENDER, attach.Role))
                .WithAssertion(attach => Assert.AreEqual(SenderSettleMode.Unsettled, attach.SndSettleMode))
                .WithAssertion(attach => Assert.AreEqual(ReceiverSettleMode.First, attach.RcvSettleMode))
                .WithAssertion(attach => Assert.NotNull(attach.Source))
                .WithAssertion(attach => targetMatcher(attach.Target as Target));

            if (sendResponse)
            {
                attachMatcher.WithOnComplete(context =>
                {
                    lastInitiatedLinkHandle = context.Command.Handle;

                    var target = CreateTargetObjectFromDescribedType(context.Command.Target);
                    target.Address = dynamicAddress;

                    var attach = new Attach()
                    {
                        Role = Role.RECEIVER,
                        SndSettleMode = SenderSettleMode.Unsettled,
                        RcvSettleMode = ReceiverSettleMode.First,
                        Handle = context.Command.Handle,
                        LinkName = context.Command.LinkName,
                        Source = context.Command.Source,
                        Target = target
                    };

                    context.SendCommand(attach);
                });

                Target CreateTargetObjectFromDescribedType(object o) => o is Target target ? target : new Target();
            }

            AddMatcher(attachMatcher);
        }

        public void ExpectDurableSubUnsubscribeNullSourceLookup(bool failLookup, bool shared, string subscriptionName, string topicName, bool hasClientId)
        {
            string expectedLinkName = subscriptionName;
            if (!hasClientId)
            {
                expectedLinkName += "|" + "global";
            }

            Action<string> linkNameMatcher = linkName => Assert.AreEqual(linkName, expectedLinkName);
            Action<Source> sourceMatcher = Assert.IsNull;
            Action<Target> targetMatcher = Assert.IsNotNull;

            Source responseSourceOverride = null;
            Symbol errorType = null;
            string errorMessage = null;
            if (failLookup)
            {
                errorType = AmqpError.NOT_FOUND;
                errorMessage = "No subscription link found";
            }
            else
            {
                responseSourceOverride = new Source()
                {
                    Address = topicName,
                    Dynamic = false,
                    Durable = (uint) TerminusDurability.UNSETTLED_STATE,
                    ExpiryPolicy = TerminusExpiryPolicy.NEVER,
                };

                if (shared)
                {
                    responseSourceOverride.Capabilities = hasClientId ? new[] { SymbolUtil.SHARED } : new[] { SymbolUtil.SHARED, SymbolUtil.GLOBAL };
                }
            }

            // If we don't have a ClientID, expect link capabilities to hint that we are trying
            // to reattach to a 'global' shared subscription.
            Action<Symbol[]> linkDesiredCapabilitiesMatcher = null;
            if (!hasClientId)
            {
                linkDesiredCapabilitiesMatcher = desiredCapabilities =>
                {
                    CollectionAssert.Contains(desiredCapabilities, SymbolUtil.SHARED);
                    CollectionAssert.Contains(desiredCapabilities, SymbolUtil.GLOBAL);
                };
            }

            ExpectReceiverAttach(
                linkNameMatcher: linkNameMatcher,
                sourceMatcher: sourceMatcher,
                targetMatcher: targetMatcher,
                settled: false,
                errorType: errorType,
                errorMessage: errorMessage,
                responseSourceOverride: responseSourceOverride,
                desiredCapabilitiesMatcher: linkDesiredCapabilitiesMatcher);
        }

        public void ExpectCoordinatorAttach(Action<object> sourceMatcher = null, bool refuseLink = false, Error error = null)
        {
            Action<object> coordinatorMatcher = Assert.IsInstanceOf<Coordinator>;
            sourceMatcher = sourceMatcher ?? Assert.IsNotNull;

            ExpectSenderAttach(sourceMatcher: sourceMatcher, targetMatcher: coordinatorMatcher, refuseLink: refuseLink, error: error);
        }

        public void ExpectDeclare(byte[] txnId)
        {
            ExpectTransfer(messageMatcher: DeclareMatcher, stateMatcher: Assert.IsNull, settled: false, sendResponseDisposition: true, responseState: new Declared() { TxnId = txnId },
                responseSettled: true);
        }

        public void ExpectDeclareButDoNotRespond()
        {
            ExpectTransfer(messageMatcher: DeclareMatcher, stateMatcher: Assert.IsNull, settled: false, sendResponseDisposition: false, responseState: null, responseSettled: false);
        }


        public void ExpectDeclareAndReject()
        {
            ExpectTransfer(messageMatcher: DeclareMatcher, stateMatcher: Assert.IsNull, responseState: new Rejected(), responseSettled: true);
        }

        void DeclareMatcher(Amqp.Message message)
        {
            Assert.IsNotNull(message);
            Assert.IsInstanceOf<AmqpValue>(message.BodySection);
            Assert.IsInstanceOf<Declare>(((AmqpValue) message.BodySection).Value);
        }

        public void ExpectDischarge(byte[] txnId, bool dischargeState)
        {
            ExpectDischarge(txnId: txnId, dischargeState: dischargeState, new Accepted());
        }

        public void ExpectDischargeButDoNotRespond(byte[] txnId, bool dischargeState)
        {
            ExpectTransfer(messageMatcher: m => DischargeMatcher(m, txnId, dischargeState),
                stateMatcher: Assert.IsNull,
                settled: false,
                sendResponseDisposition: false,
                responseState: null,
                responseSettled: true);
        }

        public void ExpectDischarge(byte[] txnId, bool dischargeState, DeliveryState responseState)
        {
            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with given response and settled disposition to indicate the outcome.
            ExpectTransfer(messageMatcher: m => DischargeMatcher(m, txnId, dischargeState),
                stateMatcher: Assert.IsNull,
                settled: false,
                sendResponseDisposition: true,
                responseState: responseState,
                responseSettled: true);
        }

        private void DischargeMatcher(Amqp.Message message, byte[] txnId, bool dischargeState)
        {
            Assert.IsNotNull(message);
            var bodySection = message.BodySection as AmqpValue;
            Assert.IsNotNull(bodySection);
            var discharge = bodySection.Value as Discharge;
            Assert.AreEqual(dischargeState, discharge.Fail);
            CollectionAssert.AreEqual(txnId, discharge.TxnId);
        }

        public void RemotelyCloseLastCoordinatorLinkOnDischarge(byte[] txnId, bool dischargeState, byte[] nextTxnId)
        {
            // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
            // and reply with given response and settled disposition to indicate the outcome.
            void DischargeMatcher(Amqp.Message message)
            {
                Assert.IsNotNull(message);
                var bodySection = message.BodySection as AmqpValue;
                Assert.IsNotNull(bodySection);
                var discharge = bodySection.Value as Discharge;
                Assert.AreEqual(dischargeState, discharge.Fail);
                CollectionAssert.AreEqual(txnId, discharge.TxnId);
            }

            ExpectTransfer(messageMatcher: DischargeMatcher, stateMatcher: Assert.IsNull, settled: false, sendResponseDisposition: false, responseState: null, responseSettled: false);

            RemotelyCloseLastCoordinatorLink(expectDetachResponse: true, closed: true, error: new Error(ErrorCode.TransactionRollback) { Description = "Discharge of TX failed." });
        }

        public void RemotelyCloseLastCoordinatorLink()
        {
            RemotelyCloseLastCoordinatorLink(expectDetachResponse: true, closed: true, error: new Error(ErrorCode.TransactionRollback) { Description = "Discharge of TX failed." });
        }

        private void RemotelyCloseLastCoordinatorLink(bool expectDetachResponse, bool closed, Error error)
        {
            lock (matchersLock)
            {
                var matcher = GetLastMatcher();
                matcher.WithOnComplete(context =>
                {
                    var detach = new Detach { Closed = true, Handle = lastInitiatedCoordinatorLinkHandle };
                    if (error != null)
                    {
                        detach.Error = error;
                    }

                    context.SendCommand(detach);
                });

                if (expectDetachResponse)
                {
                    var detachMatcher = new FrameMatcher<Detach>().WithAssertion(detach => Assert.AreEqual(closed, detach.Closed));
                    AddMatcher(detachMatcher);
                }
            }
        }

        public void PurgeExpectations()
        {
            lock (matchersLock)
            {
                matchers.Clear();
            }
        }

        public void Close(bool sendClose = false)
        {
            Logger.Info($"Closing {nameof(TestAmqpPeer)}: {this.ServerPort}");
            
            if (sendClose)
            {
                var close = new Close();
                this.testAmqpPeerRunner.Send(CONNECTION_CHANNEL, close);
            }

            testAmqpPeerRunner.Close();
        }

        public void DropAfterLastMatcher(int delay = 0)
        {
            lock (matchersLock)
            {
                var lastMatcher = GetLastMatcher();
                lastMatcher.WithOnComplete(context =>
                {
                    if (delay > 0)
                    {
                        Thread.Sleep(delay);
                    }
                    
                    this.testAmqpPeerRunner.Close();
                });
            }
        }

        public void RunAfterLastHandler(Action action)
        {
            lock (matchers)
            {
                var lastMatcher = GetLastMatcher();
                lastMatcher.WithOnComplete(_ => action());
            }
        }

        public void WaitForAllMatchersToComplete(int timeoutMillis)
        {
            Assert.True(WaitForAllMatchersToCompleteNoAssert(timeoutMillis),
                $"All matchers did not complete within the {timeoutMillis} ms timeout." +
                $" Remaining matchers count: {this.matchers.Count}." +
                $" Matchers {string.Join(" ", this.matchers.Select(x => x.ToString()))}");
        }

        public bool WaitForAllMatchersToCompleteNoAssert(int timeoutMillis)
        {
            lock (matchersLock)
            {
                this.matchersCompletedLatch = new CountdownEvent(matchers.Count);
            }

            bool result = this.matchersCompletedLatch.Wait(timeoutMillis);

            this.matchersCompletedLatch.Dispose();
            this.matchersCompletedLatch = null;

            return result;
        }

        private void AddMatcher(IMatcher matcher)
        {
            lock (matchersLock)
            {
                matchers.AddLast(matcher);
            }
        }

        private IMatcher GetFirstMatcher()
        {
            lock (matchersLock)
            {
                return matchers.Count > 0 ? matchers.First.Value : null;
            }
        }

        private void RemoveFirstMatcher()
        {
            lock (matchersLock)
            {
                matchers.RemoveFirst();
                matchersCompletedLatch?.Signal();
            }
        }

        private IMatcher GetLastMatcher()
        {
            lock (matchersLock)
            {
                return matchers.Count > 0 ? matchers.Last.Value : null;
            }
        }
    }
}