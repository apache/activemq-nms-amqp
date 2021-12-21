using System;
using System.Threading;
using System.Threading.Tasks;
using Amqp.Framing;
using Amqp.Transactions;
using Apache.NMS;
using Apache.NMS.AMQP;
using NMS.AMQP.Test.TestAmqp;
using NMS.AMQP.Test.TestAmqp.BasicTypes;
using NUnit.Framework;

namespace NMS.AMQP.Test.Integration.Async
{
    public class NMSContextEventListenersTestAsync : IntegrationTestFixture
    {
        [Test, Timeout(20_000)]
        public async Task TestRemotelyEndConnectionListenerInvoked()
        {
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                ManualResetEvent done = new ManualResetEvent(false);

                // Don't set a ClientId, so that the underlying AMQP connection isn't established yet
                var context = await EstablishNMSContextAsync(testPeer: testPeer, setClientId: false);

                // Tell the test peer to close the connection when executing its last handler
                testPeer.RemotelyCloseConnection(expectCloseResponse: true, errorCondition: ConnectionError.CONNECTION_FORCED, errorMessage: "buba");

                context.ExceptionListener += exception => done.Set();

                // Trigger the underlying AMQP connection
                await context.StartAsync();

                Assert.IsTrue(done.WaitOne(TimeSpan.FromSeconds(5)), "Connection should report failure");

                await context.CloseAsync();
            }
        }
        
        [Test, Timeout(20_000), Category("Windows")]
        public async Task TestConnectionInterruptedInvokedWhenConnectionToBrokerLost()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer())
            {
                ManualResetEvent connectionInterruptedInvoked = new ManualResetEvent(false);

                originalPeer.ExpectSaslAnonymous();
                originalPeer.ExpectOpen();
                originalPeer.ExpectBegin();
                originalPeer.ExpectBegin();

                var conFactory = new ConnectionFactory(FailoverIntegrationTest.CreateFailoverUri(null, null, originalPeer));
                var context = conFactory.CreateContext(AcknowledgementMode.AutoAcknowledge);
                context.ConnectionInterruptedListener += () => connectionInterruptedInvoked.Set();

                await context.StartAsync();
                
                originalPeer.Close();

                Assert.IsTrue(connectionInterruptedInvoked.WaitOne(TimeSpan.FromSeconds(10)));
            }
        }
        
        [Test, Timeout(20_000), Category("Windows")]
        public async Task TestConnectionResumedInvokedWhenConnectionToBrokerLost()
        {
            using (TestAmqpPeer originalPeer = new TestAmqpPeer())
            using (TestAmqpPeer finalPeer = new TestAmqpPeer())
            {
                ManualResetEvent connectionResumedInvoked = new ManualResetEvent(false);

                originalPeer.ExpectSaslAnonymous();
                originalPeer.ExpectOpen();
                originalPeer.ExpectBegin();
                originalPeer.ExpectBegin();

                finalPeer.ExpectSaslAnonymous();
                finalPeer.ExpectOpen();
                finalPeer.ExpectBegin();
                finalPeer.ExpectBegin();

                var conFactory =
                    new ConnectionFactory(
                        FailoverIntegrationTest.CreateFailoverUri(null, null, originalPeer, finalPeer));
                var context = conFactory.CreateContext();

                context.ConnectionResumedListener += () => connectionResumedInvoked.Set();

                await context.StartAsync();
                
                originalPeer.Close();
                Assert.IsTrue(connectionResumedInvoked.WaitOne(TimeSpan.FromSeconds(10)));
            }
        }
        
            
        [Test, Timeout(20_000)]
        public async Task TestProducedMessagesOnTransactedSessionCarryTxnId()
        {
            ManualResetEvent transactionRolledBackEvent = new ManualResetEvent(false);
            
            using (TestAmqpPeer testPeer = new TestAmqpPeer())
            {
                var connection = await EstablishNMSContextAsync(testPeer,acknowledgementMode:AcknowledgementMode.Transactional);
                

                testPeer.ExpectBegin();
                testPeer.ExpectCoordinatorAttach();
                await connection.StartAsync();
                
                // First expect an unsettled 'declare' transfer to the txn coordinator, and
                // reply with a declared disposition state containing the txnId.
                byte[] txnId = { 1, 2, 3, 4 };
                testPeer.ExpectDeclare(txnId);

                // ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                connection.TransactionRolledBackListener += session => transactionRolledBackEvent.Set();
                IQueue queue = await connection.GetQueueAsync("myQueue");

                // Create a producer to use in provoking creation of the AMQP transaction
                testPeer.ExpectSenderAttach();

                var producer = await connection.CreateProducerAsync();

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

                await producer.SendAsync(queue, await connection.CreateMessageAsync());

                testPeer.ExpectEnd();
                testPeer.ExpectClose();
                await connection.CloseAsync();

                testPeer.WaitForAllMatchersToComplete(1000);
                
                Assert.IsTrue(transactionRolledBackEvent.WaitOne(1000));
            }
        }
       
    }
}