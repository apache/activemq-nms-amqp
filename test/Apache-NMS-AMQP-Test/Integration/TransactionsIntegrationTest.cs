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
using System.Transactions;
using Amqp;
using Amqp.Framing;
using Amqp.Transactions;
using Apache.NMS;
using Apache.NMS.AMQP.Util;
using NMS.AMQP.Test.TestAmqp;
using NUnit.Framework;
using IConnection = Apache.NMS.IConnection;
using ISession = Apache.NMS.ISession;

namespace NMS.AMQP.Test.Integration
{
    [TestFixture]
    public class TransactionsIntegrationTest : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public void TestTransactionRolledBackOnSessionClose()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                byte[] txnId = { 5, 6, 7, 8 };
                testPeer.ExpectDeclare(txnId);

                // Closed session should roll-back the TX with a failed discharge
                testPeer.ExpectDischarge(txnId, true);
                testPeer.ExpectEnd();

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                session.Close();

                testPeer.ExpectClose();
                connection.Close();


                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestTransactionCommitFailWithEmptyRejectedDisposition()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a Declared disposition state containing the txnId.

                byte[] txnId1 = { 5, 6, 7, 8 };
                testPeer.ExpectDeclare(txnId1);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue("myQueue");

                // Create a producer to use in provoking creation of the AMQP transaction
                testPeer.ExpectSenderAttach();
                IMessageProducer producer = session.CreateProducer(queue);

                // Expect the message which was sent under the current transaction. Check it carries
                // TransactionalState with the above txnId but has no outcome. Respond with a
                // TransactionalState with Accepted outcome.
                Action<DeliveryState> stateMatcher = state =>
                {
                    Assert.IsInstanceOf<TransactionalState>(state);
                    var transactionalState = (TransactionalState) state;
                    CollectionAssert.AreEqual(txnId1, transactionalState.TxnId);
                    Assert.IsNull(transactionalState.Outcome);
                };
                testPeer.ExpectTransfer(messageMatcher: Assert.NotNull, stateMatcher: stateMatcher, responseState: new TransactionalState
                {
                    Outcome = new Accepted(),
                    TxnId = txnId1
                }, responseSettled: true);

                producer.Send(session.CreateMessage());

                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with rejected and settled disposition to indicate the commit failed
                testPeer.ExpectDischarge(txnId1, dischargeState: false, responseState: new Rejected());

                // Then expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                byte[] txnId2 = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId2);

                Assert.Catch<TransactionRolledBackException>(() => session.Commit(), "Commit operation should have failed.");

                // session should roll back on close
                testPeer.ExpectDischarge(txnId2, true);
                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestProducedMessagesAfterCommitOfSentMessagesFails()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a Declared disposition state containing the txnId.
                byte[] txnId1 = { 5, 6, 7, 8 };
                testPeer.ExpectDeclare(txnId1);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue("myQueue");

                // Create a producer to use in provoking creation of the AMQP transaction
                testPeer.ExpectSenderAttach();
                IMessageProducer producer = session.CreateProducer(queue);

                // Expect the message which was sent under the current transaction. Check it carries
                // TransactionalState with the above txnId but has no outcome. Respond with a
                // TransactionalState with Accepted outcome.
                Action<DeliveryState> stateMatcher = state =>
                {
                    Assert.IsInstanceOf<TransactionalState>(state);
                    var transactionalState = (TransactionalState) state;
                    CollectionAssert.AreEqual(txnId1, transactionalState.TxnId);
                    Assert.IsNull(transactionalState.Outcome);
                };
                testPeer.ExpectTransfer(messageMatcher: Assert.NotNull, stateMatcher: stateMatcher, responseState: new TransactionalState
                {
                    Outcome = new Accepted(),
                    TxnId = txnId1
                }, responseSettled: true);

                producer.Send(session.CreateMessage());

                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with rejected and settled disposition to indicate the commit failed
                testPeer.ExpectDischarge(txnId1, false, new Rejected() { Error = new Error(ErrorCode.InternalError) { Description = "Unknown error" } });

                // Then expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                byte[] txnId2 = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId2);

                Assert.Catch<TransactionRolledBackException>(() => session.Commit(), "Commit operation should have failed.");

                // Expect the message which was sent under the current transaction. Check it carries
                // TransactionalState with the above txnId but has no outcome. Respond with a
                // TransactionalState with Accepted outcome.
                stateMatcher = state =>
                {
                    Assert.IsInstanceOf<TransactionalState>(state);
                    var transactionalState = (TransactionalState) state;
                    CollectionAssert.AreEqual(txnId2, transactionalState.TxnId);
                    Assert.IsNull(transactionalState.Outcome);
                };
                testPeer.ExpectTransfer(messageMatcher: Assert.NotNull, stateMatcher: stateMatcher, responseState: new TransactionalState
                {
                    Outcome = new Accepted(),
                    TxnId = txnId2
                }, responseSettled: true);
                testPeer.ExpectDischarge(txnId2, dischargeState: true);

                producer.Send(session.CreateMessage());

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestProducedMessagesAfterRollbackSentMessagesFails()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a Declared disposition state containing the txnId.
                byte[] txnId1 = { 5, 6, 7, 8 };
                byte[] txnId2 = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId1);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue("myQueue");

                // Create a producer to use in provoking creation of the AMQP transaction
                testPeer.ExpectSenderAttach();
                IMessageProducer producer = session.CreateProducer(queue);

                // Expect the message which was sent under the current transaction. Check it carries
                // TransactionalState with the above txnId but has no outcome. Respond with a
                // TransactionalState with Accepted outcome.

                Action<DeliveryState> stateMatcher = state =>
                {
                    Assert.IsInstanceOf<TransactionalState>(state);
                    var transactionalState = (TransactionalState) state;
                    CollectionAssert.AreEqual(txnId1, transactionalState.TxnId);
                    Assert.IsNull(transactionalState.Outcome);
                };
                testPeer.ExpectTransfer(messageMatcher: Assert.NotNull, stateMatcher: stateMatcher, responseState: new TransactionalState
                {
                    Outcome = new Accepted(),
                    TxnId = txnId1
                }, responseSettled: true);

                producer.Send(session.CreateMessage());

                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with rejected and settled disposition to indicate the rollback failed
                testPeer.ExpectDischarge(txnId1, true, new Rejected() { Error = new Error(ErrorCode.InternalError) { Description = "Unknown error" } });

                // Then expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                testPeer.ExpectDeclare(txnId2);

                Assert.Catch<TransactionRolledBackException>(() => session.Rollback(), "Rollback operation should have failed.");

                // Expect the message which was sent under the current transaction. Check it carries
                // TransactionalState with the above txnId but has no outcome. Respond with a
                // TransactionalState with Accepted outcome.
                stateMatcher = state =>
                {
                    Assert.IsInstanceOf<TransactionalState>(state);
                    var transactionalState = (TransactionalState) state;
                    CollectionAssert.AreEqual(txnId2, transactionalState.TxnId);
                    Assert.IsNull(transactionalState.Outcome);
                };
                testPeer.ExpectTransfer(messageMatcher: Assert.NotNull, stateMatcher: stateMatcher, responseState: new TransactionalState
                {
                    Outcome = new Accepted(),
                    TxnId = txnId2
                }, responseSettled: true);
                testPeer.ExpectDischarge(txnId2, dischargeState: true);

                producer.Send(session.CreateMessage());

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCommitTransactedSessionWithConsumerReceivingAllMessages()
        {
            DoCommitTransactedSessionWithConsumerTestImpl(1, 1, false, false);
        }

        [Test, Timeout(20_000), Ignore("Until deferred close is implemented for AmqpConsumer")]
        public void TestCommitTransactedSessionWithConsumerReceivingAllMessagesAndCloseBefore()
        {
            DoCommitTransactedSessionWithConsumerTestImpl(1, 1, true, true);
        }

        [Test, Timeout(20_000)]
        public void TestCommitTransactedSessionWithConsumerReceivingAllMessagesAndCloseAfter()
        {
            DoCommitTransactedSessionWithConsumerTestImpl(1, 1, true, false);
        }

        [Test, Timeout(20_000)]
        public void TestCommitTransactedSessionWithConsumerReceivingSomeMessages()
        {
            DoCommitTransactedSessionWithConsumerTestImpl(5, 2, false, false);
        }

        [Test, Timeout(20_000), Ignore("Until deferred close is implemented for AmqpConsumer")]
        public void TestCommitTransactedSessionWithConsumerReceivingSomeMessagesAndClosesBefore()
        {
            DoCommitTransactedSessionWithConsumerTestImpl(5, 2, true, true);
        }

        [Test, Timeout(20_000), Ignore("Until deferred close is implemented for AmqpConsumer")]
        public void TestCommitTransactedSessionWithConsumerReceivingSomeMessagesAndClosesAfter()
        {
            DoCommitTransactedSessionWithConsumerTestImpl(5, 2, true, false);
        }

        private void DoCommitTransactedSessionWithConsumerTestImpl(int transferCount, int consumeCount, bool closeConsumer, bool closeBeforeCommit)
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                byte[] txnId = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), transferCount);

                for (int i = 1; i <= consumeCount; i++)
                {
                    // Then expect a *settled* TransactionalState disposition for each message once received by the consumer
                    testPeer.ExpectDisposition(settled: true, stateMatcher: state =>
                    {
                        Assert.IsInstanceOf<TransactionalState>(state);
                        var transactionalState = (TransactionalState) state;
                        Assert.AreEqual(txnId, transactionalState.TxnId);
                        Assert.IsInstanceOf<Accepted>(transactionalState.Outcome);
                    });
                }

                IMessageConsumer messageConsumer = session.CreateConsumer(queue);

                for (int i = 1; i <= consumeCount; i++)
                {
                    IMessage receivedMessage = messageConsumer.Receive(TimeSpan.FromSeconds(3));
                    Assert.NotNull(receivedMessage);
                    Assert.IsInstanceOf<ITextMessage>(receivedMessage);
                }

                // Expect the consumer to close now
                if (closeConsumer && closeBeforeCommit)
                {
                    // Expect the client to then drain off all credit from the link.
                    testPeer.ExpectLinkFlow(drain: true, sendDrainFlowResponse: true);

                    // Expect the messages that were not consumed to be released
                    int unconsumed = transferCount - consumeCount;
                    for (int i = 1; i <= unconsumed; i++)
                    {
                        testPeer.ExpectDispositionThatIsReleasedAndSettled();
                    }

                    // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                    // and reply with accepted and settled disposition to indicate the commit succeeded
                    testPeer.ExpectDischarge(txnId, dischargeState: false);

                    // Then expect an unsettled 'declare' transfer to the txn coordinator, and
                    // reply with a declared disposition state containing the txnId.
                    testPeer.ExpectDeclare(txnId);

                    // Now the deferred close should be performed.
                    testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);

                    messageConsumer.Close();
                }
                else
                {
                    // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                    // and reply with accepted and settled disposition to indicate the commit succeeded
                    testPeer.ExpectDischarge(txnId, dischargeState: false);

                    // Then expect an unsettled 'declare' transfer to the txn coordinator, and
                    // reply with a declared disposition state containing the txnId.
                    testPeer.ExpectDeclare(txnId);
                }

                session.Commit();

                if (closeConsumer && !closeBeforeCommit)
                {
                    testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);

                    // Expect the messages that were not consumed to be released
                    int unconsumed = transferCount - consumeCount;
                    for (int i = 1; i <= unconsumed; i++)
                    {
                        testPeer.ExpectDispositionThatIsReleasedAndSettled();
                    }

                    messageConsumer.Close();
                }

                testPeer.ExpectDischarge(txnId, dischargeState: true);
                testPeer.ExpectClose();

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestConsumerWithNoMessageCanCloseBeforeCommit()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                byte[] txnId = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();

                // TODO: qpid-jms extend 2 additional flow links
                // 1) Drain related with deferred consumer close, this feature is currently 
                //    not implemented.
                // 2) Consumer pull - not implemented
                // testPeer.ExpectLinkFlow(drain: true, sendDrainFlowResponse: true);
                // testPeer.ExpectLinkFlow(drain: false, sendDrainFlowResponse: false);

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);

                IMessageConsumer messageConsumer = session.CreateConsumer(queue);
                Assert.IsNull(messageConsumer.ReceiveNoWait());

                messageConsumer.Close();

                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the commit succeeded
                testPeer.ExpectDischarge(txnId, dischargeState: false);

                // Then expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                testPeer.ExpectDeclare(txnId);
                testPeer.ExpectDischarge(txnId, dischargeState: true);

                session.Commit();

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestConsumerWithNoMessageCanCloseBeforeRollback()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                byte[] txnId = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlow();

                // TODO: qpid-jms extend 2 additional flow links
                // 1) Drain related with deferred consumer close, this feature is currently 
                //    not implemented.
                // 2) Consumer pull - not implemented
                // testPeer.ExpectLinkFlow(drain: true, sendDrainFlowResponse: true);
                // testPeer.ExpectLinkFlow(drain: false, sendDrainFlowResponse: false);

                testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);

                IMessageConsumer messageConsumer = session.CreateConsumer(queue);
                Assert.IsNull(messageConsumer.ReceiveNoWait());

                messageConsumer.Close();

                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the commit succeeded
                testPeer.ExpectDischarge(txnId, dischargeState: true);

                // Then expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                testPeer.ExpectDeclare(txnId);
                testPeer.ExpectDischarge(txnId, dischargeState: true);

                session.Rollback();

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestProducedMessagesOnTransactedSessionCarryTxnId()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                byte[] txnId = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue("myQueue");

                // Create a producer to use in provoking creation of the AMQP transaction
                testPeer.ExpectSenderAttach();

                IMessageProducer producer = session.CreateProducer(queue);

                // Expect the message which was sent under the current transaction. Check it carries
                // TransactionalState with the above txnId but has no outcome. Respond with a
                // TransactionalState with Accepted outcome.
                testPeer.ExpectTransfer(messageMatcher: Assert.NotNull,
                    stateMatcher: state =>
                    {
                        Assert.IsInstanceOf<TransactionalState>(state);
                        TransactionalState transactionalState = (TransactionalState) state;
                        CollectionAssert.AreEqual(txnId, transactionalState.TxnId);
                        Assert.IsNull(transactionalState.Outcome);
                    },
                    responseState: new TransactionalState() { TxnId = txnId, Outcome = new Accepted() },
                    responseSettled: true);
                testPeer.ExpectDischarge(txnId, dischargeState: true);

                producer.Send(session.CreateMessage());

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestProducedMessagesOnTransactedSessionCanBeReused()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                byte[] txnId = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue("myQueue");

                // Create a producer to use in provoking creation of the AMQP transaction
                testPeer.ExpectSenderAttach();

                IMessageProducer producer = session.CreateProducer(queue);

                // Expect the message which was sent under the current transaction. Check it carries
                // TransactionalState with the above txnId but has no outcome. Respond with a
                // TransactionalState with Accepted outcome.
                IMessage message = session.CreateMessage();
                for (int i = 0; i < 3; i++)
                {
                    testPeer.ExpectTransfer(messageMatcher: Assert.NotNull,
                        stateMatcher: state =>
                        {
                            Assert.IsInstanceOf<TransactionalState>(state);
                            TransactionalState transactionalState = (TransactionalState) state;
                            CollectionAssert.AreEqual(txnId, transactionalState.TxnId);
                            Assert.IsNull(transactionalState.Outcome);
                        },
                        responseState: new TransactionalState() { TxnId = txnId, Outcome = new Accepted() },
                        responseSettled: true);

                    message.Properties.SetInt("sequence", i);

                    producer.Send(message);
                }

                // Expect rollback on close without a commit call.
                testPeer.ExpectDischarge(txnId, dischargeState: true);
                testPeer.ExpectClose();

                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestRollbackTransactedSessionWithConsumerReceivingAllMessages()
        {
            DoRollbackTransactedSessionWithConsumerTestImpl(1, 1, false);
        }

        [Test, Timeout(20_000), Ignore("Until deferred close is implemented for AmqpConsumer")]
        public void TestRollbackTransactedSessionWithConsumerReceivingAllMessagesThenCloses()
        {
            DoRollbackTransactedSessionWithConsumerTestImpl(1, 1, true);
        }

        [Test, Timeout(20_000), Ignore("TODO: Fix")]
        public void TestRollbackTransactedSessionWithConsumerReceivingSomeMessages()
        {
            DoRollbackTransactedSessionWithConsumerTestImpl(5, 2, false);
        }

        [Test, Timeout(20_000), Ignore("Until deferred close is implemented for AmqpConsumer")]
        public void TestRollbackTransactedSessionWithConsumerReceivingSomeMessagesThenCloses()
        {
            DoRollbackTransactedSessionWithConsumerTestImpl(5, 2, true);
        }

        private void DoRollbackTransactedSessionWithConsumerTestImpl(int transferCount, int consumeCount, bool closeConsumer)
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                byte[] txnId = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), transferCount);

                for (int i = 1; i <= consumeCount; i++)
                {
                    // Then expect a *settled* TransactionalState disposition for each message once received by the consumer
                    testPeer.ExpectDisposition(settled: true, stateMatcher: state =>
                    {
                        Assert.IsInstanceOf<TransactionalState>(state);
                        var transactionalState = (TransactionalState) state;
                        Assert.AreEqual(txnId, transactionalState.TxnId);
                        Assert.IsInstanceOf<Accepted>(transactionalState.Outcome);
                    });
                }

                IMessageConsumer messageConsumer = session.CreateConsumer(queue);

                for (int i = 1; i <= consumeCount; i++)
                {
                    IMessage receivedMessage = messageConsumer.Receive(TimeSpan.FromSeconds(3));
                    Assert.IsNotNull(receivedMessage);
                    Assert.IsInstanceOf<ITextMessage>(receivedMessage);
                }

                // Expect the consumer to be 'stopped' prior to rollback by issuing a 'drain'
                testPeer.ExpectLinkFlow(drain: true, sendDrainFlowResponse: true, creditMatcher: c => Assert.AreEqual(0, c));

                if (closeConsumer)
                {
                    // Expect the messages that were not consumed to be released
                    int unconsumed = transferCount - consumeCount;
                    for (int i = 1; i <= unconsumed; i++)
                    {
                        testPeer.ExpectDispositionThatIsReleasedAndSettled();
                    }

                    // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                    // and reply with accepted and settled disposition to indicate the commit succeeded
                    testPeer.ExpectDischarge(txnId, dischargeState: false);

                    // Then expect an unsettled 'declare' transfer to the txn coordinator, and
                    // reply with a declared disposition state containing the txnId.
                    testPeer.ExpectDeclare(txnId);

                    // Now the deferred close should be performed.
                    testPeer.ExpectDetach(expectClosed: true, sendResponse: true, replyClosed: true);

                    messageConsumer.Close();
                }
                else
                {
                    // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                    // and reply with accepted and settled disposition to indicate the rollback succeeded
                    testPeer.ExpectDischarge(txnId, dischargeState: true);

                    // Then expect an unsettled 'declare' transfer to the txn coordinator, and
                    // reply with a declared disposition state containing the txnId.
                    testPeer.ExpectDeclare(txnId);

                    // Expect the messages that were not consumed to be released
                    int unconsumed = transferCount - consumeCount;
                    for (int i = 1; i <= unconsumed; i++)
                    {
                        testPeer.ExpectDispositionThatIsReleasedAndSettled();
                    }

                    // Expect the consumer to be 'started' again as rollback completes
                    testPeer.ExpectLinkFlow(drain: false, sendDrainFlowResponse: false, creditMatcher: c => Assert.Greater(c, 0));
                }

                testPeer.ExpectDischarge(txnId, dischargeState: true);
                session.Rollback();

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        // TODO: 
        // TestRollbackTransactedSessionWithPrefetchFullBeforeStoppingConsumer
        // TestRollbackTransactedSessionWithPrefetchFullyUtilisedByDrainWhenStoppingConsumer

        [Test, Timeout(20_000)]
        public void TestDefaultOutcomeIsModifiedForConsumerSourceOnTransactedSession()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                byte[] txnId = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                string queueName = "myQueue";
                IQueue queue = session.GetQueue(queueName);

                testPeer.ExpectReceiverAttach(linkNameMatcher: Assert.IsNotNull, targetMatcher: Assert.IsNotNull, sourceMatcher: source =>
                {
                    Assert.AreEqual(queueName, source.Address);
                    Assert.IsFalse(source.Dynamic);
                    CollectionAssert.Contains(source.Outcomes, SymbolUtil.ATTACH_OUTCOME_ACCEPTED);
                    CollectionAssert.Contains(source.Outcomes, SymbolUtil.ATTACH_OUTCOME_REJECTED);
                    CollectionAssert.Contains(source.Outcomes, SymbolUtil.ATTACH_OUTCOME_RELEASED);
                    CollectionAssert.Contains(source.Outcomes, SymbolUtil.ATTACH_OUTCOME_MODIFIED);

                    Assert.IsInstanceOf<Modified>(source.DefaultOutcome);
                    Modified modified = (Modified) source.DefaultOutcome;
                    Assert.IsTrue(modified.DeliveryFailed);
                    Assert.IsFalse(modified.UndeliverableHere);
                });

                testPeer.ExpectLinkFlow();
                testPeer.ExpectDischarge(txnId, dischargeState: true);
                session.CreateConsumer(queue);

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestCoordinatorLinkSupportedOutcomes()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach(sourceMatcher: s =>
                {
                    Source source = (Source) s;
                    CollectionAssert.Contains(source.Outcomes, SymbolUtil.ATTACH_OUTCOME_ACCEPTED);
                    CollectionAssert.Contains(source.Outcomes, SymbolUtil.ATTACH_OUTCOME_REJECTED);
                    CollectionAssert.Contains(source.Outcomes, SymbolUtil.ATTACH_OUTCOME_RELEASED);
                    CollectionAssert.Contains(source.Outcomes, SymbolUtil.ATTACH_OUTCOME_MODIFIED);
                });

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                byte[] txnId = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId);

                connection.CreateSession(AcknowledgementMode.Transactional);

                //Expect rollback on close
                testPeer.ExpectDischarge(txnId, dischargeState: true);

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestRollbackErrorCoordinatorClosedOnCommit()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                byte[] txnId1 = { 5, 6, 7, 8 };
                byte[] txnId2 = { 1, 2, 3, 4 };

                testPeer.ExpectDeclare(txnId1);
                testPeer.RemotelyCloseLastCoordinatorLinkOnDischarge(txnId: txnId1, dischargeState: false, nextTxnId: txnId2);
                testPeer.ExpectCoordinatorAttach();
                testPeer.ExpectDeclare(txnId2);
                testPeer.ExpectDischarge(txnId2, dischargeState: true);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);

                Assert.Catch<TransactionRolledBackException>(() => session.Commit(), "Transaction should have rolled back");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestRollbackErrorWhenCoordinatorRemotelyClosed()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                byte[] txnId = { 5, 6, 7, 8 };
                testPeer.ExpectDeclare(txnId);
                testPeer.RemotelyCloseLastCoordinatorLink();

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);

                testPeer.WaitForAllMatchersToComplete(2000);

                testPeer.ExpectCoordinatorAttach();
                testPeer.ExpectDeclare(txnId);

                testPeer.ExpectDischarge(txnId, true);

                Assert.Catch<TransactionRolledBackException>(() => session.Commit(), "Transaction should have rolled back");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestNMSErrorCoordinatorClosedOnRollback()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                byte[] txnId1 = { 5, 6, 7, 8 };
                byte[] txnId2 = { 1, 2, 3, 4 };

                testPeer.ExpectDeclare(txnId1);
                testPeer.RemotelyCloseLastCoordinatorLinkOnDischarge(txnId: txnId1, dischargeState: true, nextTxnId: txnId2);
                testPeer.ExpectCoordinatorAttach();
                testPeer.ExpectDeclare(txnId2);
                testPeer.ExpectDischarge(txnId2, dischargeState: true);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);

                Assert.Catch<NMSException>(() => session.Rollback(), "Rollback should have thrown a NMSException");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestNMSExceptionOnRollbackWhenCoordinatorRemotelyClosed()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                byte[] txnId = { 5, 6, 7, 8 };
                testPeer.ExpectDeclare(txnId);
                testPeer.RemotelyCloseLastCoordinatorLink();

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);

                testPeer.WaitForAllMatchersToComplete(2000);

                testPeer.ExpectCoordinatorAttach();
                testPeer.ExpectDeclare(txnId);

                testPeer.ExpectDischarge(txnId, dischargeState: true);

                Assert.Catch<NMSException>(() => session.Rollback(), "Rollback should have thrown a NMSException");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSendAfterCoordinatorLinkClosedDuringTX()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a Declared disposition state containing the txnId.
                byte[] txnId = { 5, 6, 7, 8 };
                testPeer.ExpectDeclare(txnId);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue("myQueue");

                // Create a producer to use in provoking creation of the AMQP transaction
                testPeer.ExpectSenderAttach();

                // Close the link, the messages should now just get dropped on the floor.
                testPeer.RemotelyCloseLastCoordinatorLink();

                IMessageProducer producer = session.CreateProducer(queue);

                testPeer.WaitForAllMatchersToComplete(2000);

                producer.Send(session.CreateMessage());

                // Expect that a new link will be created in order to start the next TX.
                txnId = new byte[] { 1, 2, 3, 4 };
                testPeer.ExpectCoordinatorAttach();
                testPeer.ExpectDeclare(txnId);

                // Expect that the session TX will rollback on close.
                testPeer.ExpectDischarge(txnId, dischargeState: true);

                Assert.Catch<TransactionRolledBackException>(() => session.Commit(), "Commit operation should have failed.");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestReceiveAfterCoordinatorLinkClosedDuringTX()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a Declared disposition state containing the txnId.
                byte[] txnId = { 5, 6, 7, 8 };
                testPeer.ExpectDeclare(txnId);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue("myQueue");

                // Create a consumer and send it an initial message for receive to process.
                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithNullContent());

                // Close the link, the messages should now just get dropped on the floor.
                testPeer.RemotelyCloseLastCoordinatorLink();

                IMessageConsumer consumer = session.CreateConsumer(queue);

                testPeer.WaitForAllMatchersToComplete(2000);

                // receiving the message would normally ack it, since the TX is failed this
                // should not result in a disposition going out.
                IMessage received = consumer.Receive();
                Assert.IsNotNull(received);

                // Expect that a new link will be created in order to start the next TX.
                txnId = new byte[] { 1, 2, 3, 4 };
                testPeer.ExpectCoordinatorAttach();
                testPeer.ExpectDeclare(txnId);

                // Expect that the session TX will rollback on close.
                testPeer.ExpectDischarge(txnId, dischargeState: true);

                Assert.Catch<TransactionRolledBackException>(() => session.Commit(), "Commit operation should have failed.");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSessionCreateFailsOnDeclareTimeout()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer, "nms.requestTimeout=500");
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();
                testPeer.ExpectDeclareButDoNotRespond();

                // Expect the AMQP session to be closed due to the NMS session creation failure.
                testPeer.ExpectEnd();

                // TODO: Replace NMSException with sth more specific, in qpid-jms it is JmsOperationTimedOutException
                Assert.Catch<NMSException>(() => connection.CreateSession(AcknowledgementMode.Transactional), "Should have timed out waiting for declare.");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSessionCreateFailsOnDeclareRejection()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer, "nms.closeTimeout=100");
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a Rejected disposition state to indicate failure.
                testPeer.ExpectDeclareAndReject();

                // Expect the AMQP session to be closed due to the NMS session creation failure.
                testPeer.ExpectEnd();

                Assert.Catch<NMSException>(() => connection.CreateSession(AcknowledgementMode.Transactional));

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestSessionCreateFailsOnCoordinatorLinkRefusal()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer, "nms.closeTimeout=100");
                connection.Start();

                testPeer.ExpectBegin();

                // Expect coordinator link, refuse it, expect detach reply
                string errorMessage = "CoordinatorLinkRefusal-breadcrumb";
                testPeer.ExpectCoordinatorAttach(refuseLink: true, error: new Error(ErrorCode.NotImplemented) { Description = errorMessage });
                testPeer.ExpectDetach(expectClosed: true, sendResponse: false, replyClosed: false);

                // Expect the AMQP session to be closed due to the NMS session creation failure.
                testPeer.ExpectEnd();

                NMSException exception = Assert.Catch<NMSException>(() => connection.CreateSession(AcknowledgementMode.Transactional));
                Assert.IsTrue(exception.Message.Contains(errorMessage), "Expected exception message to contain breadcrumb");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestTransactionRolledBackOnSessionCloseTimesOut()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer, "nms.requestTimeout=500");
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                byte[] txnId = { 5, 6, 7, 8 };
                testPeer.ExpectDeclare(txnId);

                // Closed session should roll-back the TX with a failed discharge
                testPeer.ExpectDischargeButDoNotRespond(txnId, dischargeState: true);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);

                // TODO: Replace NMSException with sth more specific, in qpid-jms it is JmsOperationTimedOutException
                Assert.Catch<NMSException>(() => session.Close(), "Should have timed out waiting for discharge.");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestTransactionRolledBackTimesOut()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer, "nms.requestTimeout=500");
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                byte[] txnId1 = { 5, 6, 7, 8 };
                byte[] txnId2 = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId1);

                // Expect discharge but don't respond so that the request timeout kicks in and fails
                // the discharge.  The pipelined declare should arrive as well and be discharged as the
                // client attempts to recover to a known good state.
                testPeer.ExpectDischargeButDoNotRespond(txnId1, dischargeState: true);

                // Session should throw from the rollback and then try and recover.
                testPeer.ExpectDeclare(txnId2);
                testPeer.ExpectDischarge(txnId2, dischargeState: true);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);

                // TODO: Replace NMSException with sth more specific, in qpid-jms it is JmsOperationTimedOutException
                Assert.Catch<NMSException>(() => session.Rollback(), "Should have timed out waiting for discharge.");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestTransactionCommitTimesOut()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer, "nms.requestTimeout=500");
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                byte[] txnId1 = { 5, 6, 7, 8 };
                byte[] txnId2 = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId1);

                // Expect discharge but don't respond so that the request timeout kicks in and fails
                // the discharge.  The pipelined declare should arrive as well and be discharged as the
                // client attempts to recover to a known good state.
                testPeer.ExpectDischargeButDoNotRespond(txnId1, dischargeState: false);
                testPeer.ExpectDeclare(txnId2);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);

                // TODO: Replace NMSException with sth more specific, in qpid-jms it is JmsOperationTimedOutException
                Assert.Catch<NMSException>(() => session.Commit(), "Should have timed out waiting for discharge.");

                // Session rolls back on close
                testPeer.ExpectDischarge(txnId2, dischargeState: true);

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000), Ignore("TODO: Fix")]
        public void TestTransactionCommitTimesOutAndNoNextBeginTimesOut()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer, "nms.requestTimeout=500");
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                byte[] txnId1 = { 5, 6, 7, 8 };
                byte[] txnId2 = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId1);

                // Expect discharge and don't respond so that the request timeout kicks in
                // Expect pipelined declare and don't response so that the request timeout kicks in.
                // The commit operation should throw a timed out exception at that point.
                testPeer.ExpectDischargeButDoNotRespond(txnId1, dischargeState: false);
                testPeer.ExpectDeclareButDoNotRespond();

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);

                // After the pipelined operations both time out, the session should attempt to
                // recover by creating a new TX, then on close the session should roll it back
                testPeer.ExpectDeclare(txnId2);
                testPeer.ExpectDischarge(txnId2, dischargeState: true);

                // TODO: Replace NMSException with sth more specific, in qpid-jms it is JmsOperationTimedOutException
                Assert.Catch<NMSException>(() => session.Commit(), "Should have timed out waiting for discharge.");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000), Ignore("TODO: Fix")]
        public void TestRollbackWithNoResponseForSuspendConsumer()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                byte[] txnId = { 5, 6, 7, 8 };
                testPeer.ExpectDeclare(txnId);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue("myQueue");

                testPeer.ExpectReceiverAttach();
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithContent(), count: 1);

                // Then expect a *settled* TransactionalState disposition for the message once received by the consumer
                testPeer.ExpectDisposition(settled: true, state =>
                {
                    var transactionalState = (TransactionalState) state;
                    CollectionAssert.AreEqual(txnId, transactionalState.TxnId);
                    Assert.IsInstanceOf<Accepted>(transactionalState.Outcome);
                });

                // Read one so we try to suspend on rollback
                IMessageConsumer messageConsumer = session.CreateConsumer(queue);
                IMessage receivedMessage = messageConsumer.Receive(TimeSpan.FromSeconds(3));

                Assert.NotNull(receivedMessage);
                Assert.IsInstanceOf<ITextMessage>(receivedMessage);

                // Expect the consumer to be 'stopped' prior to rollback by issuing a 'drain'
                testPeer.ExpectLinkFlow(drain: true, sendDrainFlowResponse: false);

                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the rollback succeeded
                testPeer.ExpectDischarge(txnId, dischargeState: true);

                testPeer.ExpectDeclare(txnId);
                testPeer.ExpectDischarge(txnId, dischargeState: true);

                Assert.Catch<NMSException>(() => session.Rollback(), "Should throw a timed out exception");

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(1000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestConsumerMessageOrderOnTransactedSession()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                int messageCount = 10;

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                byte[] txnId = { 5, 6, 7, 8 };
                testPeer.ExpectDeclare(txnId);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue("myQueue");

                // Expect the browser enumeration to create a underlying consumer
                testPeer.ExpectReceiverAttach();

                // Expect initial credit to be sent, respond with some messages that are tagged with
                // a sequence number we can use to determine if order is maintained.
                testPeer.ExpectLinkFlowRespondWithTransfer(CreateMessageWithNullContent(), count: messageCount, addMessageNumberProperty: true);

                for (int i = 1; i <= messageCount; i++)
                {
                    // Then expect an *settled* TransactionalState disposition for each message once received by the consumer
                    testPeer.ExpectSettledTransactionalDisposition(txnId);
                }

                IMessageConsumer consumer = session.CreateConsumer(queue);
                for (int i = 0; i < messageCount; i++)
                {
                    IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(500));
                    Assert.IsNotNull(message);
                    Assert.AreEqual(i, message.Properties.GetInt(TestAmqpPeer.MESSAGE_NUMBER));
                }

                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the rollback succeeded
                testPeer.ExpectDischarge(txnId, true);
                testPeer.ExpectEnd();

                session.Close();

                testPeer.ExpectClose();
                connection.Close();

                testPeer.WaitForAllMatchersToComplete(3000);
            }
        }

        [Test, Timeout(20_000)]
        public void TestConsumeManyWithSingleTXPerMessage()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                IConnection connection = EstablishConnection(testPeer);
                connection.Start();

                int messageCount = 10;

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();

                var txnIdQueue = new Queue<byte[]>(3);
                txnIdQueue.Enqueue(new byte[] { 1, 2, 3, 4 });
                txnIdQueue.Enqueue(new byte[] { 2, 4, 6, 8 });
                txnIdQueue.Enqueue(new byte[] { 5, 4, 3, 2 });

                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                byte[] txnId = txnIdQueue.Dequeue();
                txnIdQueue.Enqueue(txnId);
                testPeer.ExpectDeclare(txnId);

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue("myQueue");

                // Expect the browser enumeration to create a underlying consumer
                testPeer.ExpectReceiverAttach();

                // Expect initial credit to be sent, respond with some messages that are tagged with
                // a sequence number we can use to determine if order is maintained.
                testPeer.ExpectLinkFlowRespondWithTransfer(message: CreateMessageWithNullContent(), count: messageCount, addMessageNumberProperty: true);

                IMessageConsumer consumer = session.CreateConsumer(queue);

                for (int i = 0; i < messageCount; i++)
                {
                    // Then expect an *settled* TransactionalState disposition for each message once received by the consumer
                    testPeer.ExpectSettledTransactionalDisposition(txnId);

                    IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(500));
                    Assert.NotNull(message);
                    Assert.AreEqual(i, message.Properties.GetInt(TestAmqpPeer.MESSAGE_NUMBER));

                    // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                    // and reply with accepted and settled disposition to indicate the commit succeeded
                    testPeer.ExpectDischarge(txnId, dischargeState: false);

                    // Expect the next transaction to start.
                    txnId = txnIdQueue.Dequeue();
                    txnIdQueue.Enqueue(txnId);
                    testPeer.ExpectDeclare(txnId);
                    
                    session.Commit();
                }
                
                // Expect an unsettled 'discharge' transfer to the txn coordinator containing the txnId,
                // and reply with accepted and settled disposition to indicate the rollback succeeded
                testPeer.ExpectDischarge(txnId, dischargeState: true);
                testPeer.ExpectEnd();

                session.Close();
                
                testPeer.ExpectClose();
                connection.Close();
                
                testPeer.WaitForAllMatchersToComplete(3000);
            }
        }
    }
}