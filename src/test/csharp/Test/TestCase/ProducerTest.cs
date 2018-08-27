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
using System.Text;
using System.Collections.Specialized;
using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using Apache.NMS;
using Apache.NMS.Util;
using Apache.NMS.AMQP.Test.Util;
using Apache.NMS.AMQP.Test.Attribute;

namespace Apache.NMS.AMQP.Test.TestCase
{
    [TestFixture]
    class ProducerTest : BaseTestCase
    {

        const int TIMEOUT = 15000; // 15 secs

        public override void Setup()
        {
            base.Setup();
            waiter = new System.Threading.ManualResetEvent(false);
        }
        
        [Test]
        [ConnectionSetup(null, "c1")]
        [SessionSetup("c1", "s1")]
        [QueueSetup("s1","q1", Name = "nms.unique.queue")]
        [ProducerSetup("s1", "q1","sender", DeliveryMode = MsgDeliveryMode.NonPersistent)]
        public void TestProducerSend()
        {
            const int NUM_MSGS = 100;

            try
            {
                using(IConnection connection = GetConnection("c1"))
                using(IMessageProducer producer = GetProducer("sender"))
                {
                    connection.ExceptionListener += DefaultExceptionListener;
                    ITextMessage textMessage = producer.CreateTextMessage();
                    for(int i=0; i<NUM_MSGS; i++)
                    {
                        textMessage.Text = "msg:" + i;
                        producer.Send(textMessage);

                    }
                    waiter.WaitOne(2000); // wait 2s for message to be received by broker.
                    Assert.IsNull(asyncEx, "Received asynchronous exception. Message : {0}", asyncEx?.Message);
                    
                }
            }
            catch(Exception ex)
            {
                this.PrintTestFailureAndAssert(GetTestMethodName(), "Unexpected exception.", ex);
            }
        }

        [Test]
        [ConnectionSetup(null, "c1")]
        [SessionSetup("c1", "s1")]
        [QueueSetup("s1", "q1", Name = "nmsQueue1")]
        [QueueSetup("s1", "q2", Name = "nmsQueue2")]
        [TopicSetup("s1", "t1", Name = "nmsTopic1")]
        [TopicSetup("s1", "t2", Name = "nmsTopic2")]
        [ConsumerSetup("s1", "q1", "cq1")]
        [ConsumerSetup("s1", "q2", "cq2")]
        [ConsumerSetup("s1", "t1", "ct1")]
        [ConsumerSetup("s1", "t2", "ct2")]
        public void TestAnonymousProducerSend()
        {
            const int NUM_MSGS = 100;
            IList<IDestination> destinations = this.GetDestinations(new string[] { "q1", "q2", "t1", "t2" });
            IList<IMessageConsumer> consumers = this.GetConsumers(new string[] { "cq1", "cq2", "ct1", "ct2" });
            int DestinationPoolSize = destinations.Count;
            int ConsumerPoolSize = consumers.Count;
            
            IMessageProducer producer = null;
            
            using (ISession session = GetSession("s1"))
            using (IConnection connection = GetConnection("c1"))
            {
                try
                {
                    connection.ExceptionListener += DefaultExceptionListener;
                    foreach(IMessageConsumer c in consumers)
                    {
                        c.Listener += CreateListener(NUM_MSGS);
                    }

                    connection.Start();
                    
                    producer = session.CreateProducer();
                    producer.DeliveryMode = MsgDeliveryMode.Persistent;
                    ITextMessage textMessage = producer.CreateTextMessage();

                    for (int i = 0; i < NUM_MSGS; )
                    {
                        foreach (IDestination dest in destinations)
                        {
                            Logger.Info("Sending message " + i + " to destination " + dest.ToString());
                            textMessage.Text = "Num:" + dest.ToString() + ":" + i;
                            i++;

                            producer.Send(dest, textMessage);
                        }
                    }

                    Assert.IsTrue(waiter.WaitOne(TIMEOUT), "Failed to received all messages. Received {0} of {1} in {2}ms.", msgCount, NUM_MSGS, TIMEOUT);

                }
                catch (Exception ex)
                {
                    this.PrintTestFailureAndAssert(GetTestMethodName(), "Unexpected exception.", ex);
                }
            }
            
        }

        [Test]
        //[Repeat(25)]
        [ConnectionSetup(null,"c1")]
        [SessionSetup("c1","s1")]
        [TopicSetup("s1","t1",Name = "nms.topic.test")]
        [ConsumerSetup("s1","t1","drain")]
        public void TestMultipleProducerCreateAndSend(
            [Values(MsgDeliveryMode.NonPersistent, MsgDeliveryMode.Persistent)]
            MsgDeliveryMode mode
            )
        {
            const int MSG_TTL_MILLIS = 8500; // 8.5 secs
            const int NUM_MSGS = 200;
            const int NUM_PRODUCERS = 5;
            const string MSG_ID_KEY = "MsgIndex";
            const string PRODUCER_ID_KEY = "ProducerIndex";
            const string PRODUCER_INDEXED_ID_KEY = "ProducerIndexedMsgId";
            bool persistent = mode.Equals(MsgDeliveryMode.Persistent);
            bool useMsgId = !persistent;
            int msgIdWindow = 0;
            
            string failureErr = null;
            
            IMessageProducer producer = null;
            IList<IMessageProducer> producers = null;
            IList<int> lastProducerIndexedIds = null;
            try
            {
                using (IConnection connection = this.GetConnection("c1"))
                using (ISession session = this.GetSession("s1"))
                using (IDestination destination = this.GetDestination("t1"))
                using (IMessageConsumer drain = this.GetConsumer("drain"))
                {
                    lastProducerIndexedIds = new List<int>();
                    MessageListener ackCallback = CreateListener(NUM_MSGS);
                        
                    drain.Listener += (message) =>
                    {
                        if (failureErr == null)
                        {
                            ackCallback(message);
                            int id = message.Properties.GetInt(PRODUCER_INDEXED_ID_KEY);
                            int prodIndex = message.Properties.GetInt(PRODUCER_ID_KEY);
                            int lastId = lastProducerIndexedIds[prodIndex];
                            int advancedMsgs = id - lastId;
                            if (id < lastId)
                            {
                                failureErr = string.Format(
                                    "Received message out of order." +
                                    " Received, sent from producer {0} msg id {1} where last msg id {2}",
                                    prodIndex,
                                    id,
                                    lastId
                                    );
                                this.waiter.Set();
                            }
                            else if(persistent && advancedMsgs > 1)
                            {
                                failureErr = string.Format(
                                    "Persistent Messages where drop." +
                                    " Received, sent from producer {0} msg id {1} where last msg id {2}",
                                    prodIndex,
                                    id,
                                    lastId
                                    );
                                this.waiter.Set();
                            }
                            else
                            {
                                lastProducerIndexedIds[prodIndex] = id;
                                if (advancedMsgs > 1 && (Logger.IsInfoEnabled || Logger.IsDebugEnabled))
                                {
                                    Logger.Info(string.Format(
                                            "{0} Messages dropped for producer {1} from message id {2}", 
                                            advancedMsgs, prodIndex, lastId
                                            ));
                                }
                                msgIdWindow += advancedMsgs;
                                if (!persistent && msgIdWindow == NUM_MSGS)
                                {
                                    this.waiter.Set();
                                }
                            }
                        }
                    };
                    
                    connection.ExceptionListener += DefaultExceptionListener;
                    
                    producers = new List<IMessageProducer>();
                    for (int i = 0; i < NUM_PRODUCERS; i++)
                    {
                        try
                        {
                            producer = session.CreateProducer(destination);
                        }
                        catch (Exception ex)
                        {
                            this.PrintTestFailureAndAssert(this.GetMethodName(), "Failed to Created Producer " + i, ex);
                        }
                        producer.DeliveryMode = mode;
                        producer.DisableMessageID = !useMsgId;
                        producer.TimeToLive = TimeSpan.FromMilliseconds(MSG_TTL_MILLIS);
                        producers.Add(producer);
                        lastProducerIndexedIds.Add(-1);
                    }

                    connection.Start();

                    Assert.AreEqual(NUM_PRODUCERS, producers.Count, "Did not create all producers.");
                    Assert.IsNull(asyncEx, 
                        "Exception Listener Called While creating producers. With exception {0}.", 
                        asyncEx);
                    
                    ITextMessage msg = session.CreateTextMessage();
                    int producerIndex = -1;
                    for (int i = 0; i < NUM_MSGS; i++)
                    {
                        msg.Text = "Index:" + i;
                        msg.Properties[MSG_ID_KEY] = i;
                        msg.Properties[PRODUCER_INDEXED_ID_KEY] = i / NUM_PRODUCERS;
                        producerIndex = i % NUM_PRODUCERS;
                        msg.Properties[PRODUCER_ID_KEY] = producerIndex;
                        producers[producerIndex].Send(msg);
                    }

                    Assert.IsNull(asyncEx, "Exception Listener Called While sending messages. With exception {0}.", asyncEx);

                    Assert.IsTrue(waiter.WaitOne(TIMEOUT), 
                        "Failed to received all messages in {0}ms. Received {1} of {2} messages", 
                        TIMEOUT, msgCount, NUM_MSGS);

                    Assert.IsNull(failureErr, 
                        "Received assertion failure from IMessageConsumer message Listener. Failure : {0}", 
                        failureErr ?? "");

                    if (persistent)
                    {
                        Assert.AreEqual(NUM_MSGS, msgCount, 
                            "Receive unexpected from messages sent. Message Window {0}", msgIdWindow);
                    }
                    else
                    {
                        int missedMsgs = (msgIdWindow - msgCount);
                        Assert.AreEqual(NUM_MSGS, msgIdWindow, 
                            "Failed to receive all messages." + 
                            " Received {0} of {1} messages, with missed messages {2}, in {3}ms",
                            msgCount, NUM_MSGS, missedMsgs, TIMEOUT
                            );
                        if(missedMsgs > 0)
                        {
                            System.Text.StringBuilder sb = new System.Text.StringBuilder();
                            const string SEPARATOR = ", ";
                            for(int i=0; i<NUM_PRODUCERS; i++)
                            {
                                sb.AppendFormat("Last received Producer {0} message id  {1}{2}", i, lastProducerIndexedIds[i], SEPARATOR);
                            }
                            sb.Length = sb.Length - SEPARATOR.Length;
                            
                            Logger.Warn(string.Format("Did not receive all Non Persistent messages. Received {0} of {1} messages. Where last received message ids = [{2}]", msgCount, NUM_MSGS, sb.ToString()));
                        }
                    }

                    Assert.IsNull(asyncEx, "Exception Listener Called While receiveing messages. With exception {0}.", asyncEx);
                    //
                    // Some brokers are sticklers for detail and actually honor the 
                    // batchable flag that AMQPnetLite sets on all published messages. As
                    // a result all messages can be long received before the published 
                    // messages are acknowledged. So to avoid a hand full of 
                    // amqp:message:released outcomes, just pause a few seconds before
                    // closing the producer
                    System.Threading.Thread.Sleep(3000);
                }
            }
            catch (Exception e)
            {
                this.PrintTestFailureAndAssert(this.GetMethodName(), "Unexpected Exception.", e);
            }
            finally
            {
                if(producers != null)
                {
                    foreach(IMessageProducer p in producers)
                    {
                        p?.Close();
                        p?.Dispose();
                    }
                    producers.Clear();
                }
            }
        }
        
        #region Destination Tests

        private void TestDestinationMessageDelivery(
            IConnection connection, 
            ISession session, 
            IMessageProducer producer, 
            IDestination destination, 
            int msgPoolSize, 
            bool isDurable = false)
        {
            bool cleaned = !isDurable;
            const string PROP_KEY = "send_msg_id";
            string subName = DURABLE_SUBSRIPTION_NAME;

            int TotalMsgSent = 0;
            int TotalMsgRecv = 0;
            MsgDeliveryMode initialMode = producer.DeliveryMode;
            try
            {
                
                IMessageConsumer consumer = isDurable 
                    ? 
                    session.CreateDurableConsumer(destination as ITopic, subName, null, false) 
                    : 
                    session.CreateConsumer(destination);
                ITextMessage sendMessage = session.CreateTextMessage();

                consumer.Listener += CreateListener(msgPoolSize);
                connection.ExceptionListener += DefaultExceptionListener;

                connection.Start();

                for (int i = 0; i < msgPoolSize; i++)
                {
                    sendMessage.Text = "Msg:" + i;
                    sendMessage.Properties.SetInt(PROP_KEY, TotalMsgSent);
                    producer.Send(sendMessage);
                    TotalMsgSent++;
                }
                
                bool signal = waiter.WaitOne(TIMEOUT);
                TotalMsgRecv = msgCount;
                Assert.IsTrue(signal,
                    "Timed out waiting to receive messages. Received {0} of {1} in {2}ms.",
                    msgCount, TotalMsgSent, TIMEOUT);
                Assert.AreEqual(TotalMsgSent, msgCount,
                    "Failed to receive all messages. Received {0} of {1} in {2}ms.",
                    msgCount, TotalMsgSent, TIMEOUT);
                
                // close consumer
                consumer.Close();
                // reset waiter
                waiter.Reset();

                for (int i = 0; i < msgPoolSize; i++)
                {
                    sendMessage.Text = "Msg:" + i;
                    sendMessage.Properties.SetInt(PROP_KEY, TotalMsgSent);
                    producer.Send(sendMessage);
                    TotalMsgSent++;
                    if(isDurable || destination.IsQueue)
                    {
                        TotalMsgRecv++;
                    }
                }
                // Must stop connection before we can add a consumer
                connection.Stop();
                int expectedId = (isDurable || destination.IsQueue) ? msgPoolSize : TotalMsgSent;

                // expectedMsgCount is 2 msgPoolSize groups for non-durable topics, one for initial send of pool size and one for final send of pool size.
                // expedtedMsgCount is 3 msgPoolSize groups for queues and durable topics, same two groups for non-durable topic plus the group sent while there is no active consumer.
                int expectedMsgCount = (isDurable || destination.IsQueue) ? 3 * msgPoolSize : 2 * msgPoolSize;

                MessageListener callback = CreateListener(expectedMsgCount);
                string errString = null;
                consumer = consumer = isDurable
                    ?
                    session.CreateDurableConsumer(destination as ITopic, subName, null, false)
                    :
                    session.CreateConsumer(destination);
                consumer.Listener += (m) =>
                {
                    int id = m.Properties.GetInt(PROP_KEY);
                    if (id != expectedId)
                    {
                        errString = string.Format("Received Message with unexpected msgId. Received msg : {0} Expected : {1}", id, expectedId);
                        waiter.Set();
                        return;
                    }
                    else
                    {
                        expectedId++;
                    }
                    callback(m);
                };
                // Start Connection
                connection.Start();
                for (int i = 0; i < msgPoolSize; i++)
                {
                    sendMessage.Text = "Msg:" + i;
                    sendMessage.Properties.SetInt(PROP_KEY, TotalMsgSent);
                    producer.Send(sendMessage);
                    TotalMsgSent++;
                    TotalMsgRecv++;
                }
                
                signal = waiter.WaitOne(TIMEOUT);
                Assert.IsNull(asyncEx, "Received asynchrounous exception. Message: {0}", asyncEx?.Message);
                Assert.IsNull(errString, "Failure occured on Message Callback. Message : {0}", errString ?? "");
                Assert.IsTrue(signal, "Timed out waiting for message receive. Received {0} of {1} in {2}ms.", msgCount, TotalMsgRecv, TIMEOUT);
                Assert.AreEqual(TotalMsgRecv, msgCount, 
                    "Failed to receive all messages. Received {0} of {1} in {2}ms.", 
                    msgCount, TotalMsgRecv, TIMEOUT);
                connection.Stop();
                consumer.Close();
                
            }
            catch (Exception ex)
            {
                this.PrintTestFailureAndAssert(this.GetTestMethodName(), "Unexpected Exception", ex);
            }
            finally
            {
                if (!cleaned)
                {
                    try
                    {
                        session.DeleteDurableConsumer(subName);
                    }
                    catch (InvalidDestinationException ide)
                    {
                        Logger.Info(string.Format("Unable to unsubscribe from {0}, Cause : {1}", subName, ide));
                    }
                    catch (Exception ex)
                    {
                        Logger.Warn(string.Format("Caught unexpected failure while unsubscribing from {0}. Failure : {1}", subName, ex));
                    }
                }
            }
        }

        #region Queue Destination Tests

        [Test]
        [Repeat(30)]
        [ConnectionSetup(null, "c1")]
        [SessionSetup("c1", "s1")]
        [QueueSetup("s1", "q1", Name = "nms.queue")]
        [ProducerSetup("s1", "q1", "sender", DeliveryMode = MsgDeliveryMode.NonPersistent)]
        public void TestQueueMessageDelivery()
        {
            const int NUM_MSGS = 100;

            IDestination destination = GetDestination("q1");

            using (IConnection connection = GetConnection("c1"))
            using (ISession session = GetSession("s1"))
            using (IMessageProducer producer = GetProducer("sender"))
            {
                TestDestinationMessageDelivery(connection, session, producer, destination, NUM_MSGS);
            }

        }

        #endregion // end queue tests

        #region Topic Tests

        /*
         * Test topic Consumer message delivery reliability. This test expects the 
         * messages sent on a topic without an active consumer to be dropped.
         */
        [Test]
        [ConnectionSetup(null, "c1")]
        [SessionSetup("c1", "s1")]
        [TopicSetup("s1", "t1", Name = "nms.test")]
        [ProducerSetup("s1", "t1", "sender", DeliveryMode = MsgDeliveryMode.NonPersistent)]
        public void TestTopicMessageDelivery()
        {
            const int NUM_MSGS = 100;

            IDestination destination = GetDestination("t1");

            using (IConnection connection = GetConnection("c1"))
            using (ISession session = GetSession("s1"))
            using (IMessageProducer producer = GetProducer("sender"))
            {
                TestDestinationMessageDelivery(connection, session, producer, destination, NUM_MSGS);
            }

        }

        /*
         * Test Durable Topic Consumer message delivery reliability. This test expects the 
         * messages sent while the consumer is inactive to be retained and delivered to the
         * consumer once the subscription is active again.
         */
        [Test]
        [Repeat(20)]
        [ConnectionSetup(null, "c1")]
        [SessionSetup("c1", "s1")]
        [TopicSetup("s1", "t1", Name = DURABLE_TOPIC_NAME)]
        [ProducerSetup("s1", "t1", "sender", DeliveryMode = MsgDeliveryMode.NonPersistent)]
        public void TestDurableTopicMessageDelivery()
        {
            const int NUM_MSGS = 100;

            IDestination destination = GetDestination("t1");

            using (IConnection connection = GetConnection("c1"))
            using (ISession session = GetSession("s1"))
            using (IMessageProducer producer = GetProducer("sender"))
            {
                TestDestinationMessageDelivery(connection, session, producer, destination, NUM_MSGS, true);
            }

        }

        #endregion // end topic tests

        #region Temporary Destination Tests

        [Test]
        [ConnectionSetup(null, "c1")]
        [SessionSetup("c1", "s1")]
        [TemporaryQueueSetup("s1", "temp1")]
        [ProducerSetup("s1", "temp1", "sender", DeliveryMode = MsgDeliveryMode.NonPersistent)]
        [SkipTestOnRemoteBrokerProperties("c1", RemotePlatform = NMSTestConstants.NMS_SOLACE_PLATFORM)]
        public void TestTemporaryQueueMessageDelivery()
        {
            const int NUM_MSGS = 100;

            IDestination destination = GetDestination("temp1");

            using (IConnection connection = GetConnection("c1"))
            using (ISession session = GetSession("s1"))
            using (IMessageProducer producer = GetProducer("sender"))
            {
                TestDestinationMessageDelivery(connection, session, producer, destination, NUM_MSGS);
            }

        }

        [Test]
        [ConnectionSetup(null, "c1")]
        [SessionSetup("c1","s1")]
        [TemporaryTopicSetup("s1", "temp1")]
        [ProducerSetup("s1", "temp1", "sender", DeliveryMode = MsgDeliveryMode.NonPersistent)]
        public void TestTemporaryTopicMessageDelivery()
        {
            const int NUM_MSGS = 100;
            
            IDestination destination = GetDestination("temp1");

            using (IConnection connection = GetConnection("c1"))
            using (ISession session = GetSession("s1"))
            using (IMessageProducer producer = GetProducer("sender"))
            {
                TestDestinationMessageDelivery(connection, session, producer, destination, NUM_MSGS);
            }
            
        }


        [Test]
        [ConnectionSetup(null, "c1")]
        [SessionSetup("c1", "s1")]
        [TemporaryTopicSetup("s1", "temp1")]
        [ProducerSetup("s1", "temp1", "sender", DeliveryMode = MsgDeliveryMode.NonPersistent)]
        public void TestCannotSendOnDeletedTemporaryTopic()
        {
            
            try
            {
                using (IConnection connection = GetConnection("c1"))
                using (IDestination destination = GetDestination("temp1"))
                using (IMessageProducer producer = GetProducer("sender"))
                {
                    ITemporaryTopic tempTopic = destination as ITemporaryTopic;
                    Assert.NotNull(tempTopic, "Failed to Create Temporary Topic.");
                    IMessage msg = producer.CreateMessage();
                    tempTopic.Delete();
                    try
                    {
                        producer.Send(msg);
                        Assert.Fail("Expected Exception for sending message on deleted temporary topic.");
                    }
                    catch(NMSException nex)
                    {
                        Assert.IsTrue(nex is InvalidDestinationException, "Received Unexpected exception {0}", nex);
                    }
                    
                    
                }

            }
            catch(Exception ex)
            {
                this.PrintTestFailureAndAssert(GetTestMethodName(), "Unexpected exception.", ex);
            }
        }

        [Test]
        [ConnectionSetup(null, "c1")]
        [SessionSetup("c1", "s1", AckMode = AcknowledgementMode.IndividualAcknowledge)]
        [SessionSetup("c1", "s2")]
        [TopicSetup("s1", "t1", Name = "nms.t.temp.reply.to.topic")]
        [TemporaryTopicSetup("s1", "temp1")]
        [ProducerSetup("s1", "t1", "sender", DeliveryMode = MsgDeliveryMode.NonPersistent)]
        [ConsumerSetup("s2", "t1", "receiver")]
        [ProducerSetup("s2", "temp1", "replyer", DeliveryMode = MsgDeliveryMode.Persistent, TimeToLive = 2500)]
        [ConsumerSetup("s1", "temp1", "listener")]
        public void TestTemporaryTopicReplyTo()
        {
            const int NUM_MSGS = 100;
            const string MSG_BODY = "num : ";
            IDestination replyTo = GetDestination("temp1");
            long repliedCount = 0;
            long lastRepliedId = -1;
            string errString = null;
            CountDownLatch replierFinished = new CountDownLatch(NUM_MSGS);
            

            using (IConnection connection = GetConnection("c1"))
            using (IMessageConsumer receiver = GetConsumer("receiver"))
            using (IMessageConsumer listener = GetConsumer("listener"))
            using (IMessageProducer sender = GetProducer("sender"))
            using (IMessageProducer replyer = GetProducer("replyer"))
            {
                try
                {
                    connection.ExceptionListener += DefaultExceptionListener;
                    ITextMessage rmsg = null;
                    ITextMessage sendMsg = sender.CreateTextMessage();
                    sendMsg.NMSReplyTo = replyTo;

                    listener.Listener += (message) => 
                    {
                        if (errString == null)
                        {
                            repliedCount++;
                            long msgId = ExtractMsgId(message.NMSMessageId);
                            if (msgId != lastRepliedId + 1)
                            {
                                // Test failed release blocked thread for shutdown.
                                errString = String.Format("Received msg {0} out of order expected {1}", msgId, lastRepliedId + 1);
                                waiter.Set();
                            }
                            else
                            {
                                lastRepliedId = msgId;
                                if (msgId == NUM_MSGS - 1)
                                {
                                    message.Acknowledge();
                                    // test done signal complete.
                                    waiter.Set();
                                    return;
                                }
                                message.Acknowledge();
                            }
                        }
                    };

                    receiver.Listener += (message) =>
                    {
                        if (errString == null)
                        {
                            msgCount++;
                            rmsg = message as ITextMessage;
                            if (rmsg == null)
                            {
                                // test failure
                                errString = string.Format(
                                    "Received message, id = {2}, body of type {0}, expected {1}.", 
                                    message.GetType().Name, 
                                    typeof(ITextMessage).Name, 
                                    ExtractMsgId(message.NMSMessageId)
                                    );
                                waiter.Set();
                                return;
                            }
                            IDestination replyDestination = message.NMSReplyTo;
                            if (!replyDestination.Equals(replyTo))
                            {
                                // test failure
                                errString = string.Format(
                                    "Received message, id = {0}, with incorrect reply Destination. Expected : {1}, Actual : {2}.",
                                    ExtractMsgId(message.NMSMessageId),
                                    replyTo,
                                    replyDestination
                                    );
                                waiter.Set();
                                return;
                            }
                            else
                            {
                                ITextMessage reply = replyer.CreateTextMessage();
                                reply.Text = "Received:" + rmsg.Text;
                                try
                                {
                                    replyer.Send(reply);
                                    replierFinished.countDown();
                                }
                                catch (NMSException nEx)
                                {
                                    Logger.Error("Failed to send message from replyer Cause : " + nEx);
                                    throw nEx;
                                }
                            }
                        }
                    };

                    connection.Start();

                    for(int i=0; i<NUM_MSGS; i++)
                    {
                        sendMsg.Text = MSG_BODY + i;
                        sender.Send(sendMsg);
                    }

                    // allow for two seconds for each message to be sent and replied to.
                    int timeout = 2000 * NUM_MSGS;
                    if(!waiter.WaitOne(timeout))
                    {
                        Assert.Fail("Timed out waiting on message delivery to complete. Received {1} of {0}, Replied {2} of {0}, Last Replied Msg Id {3}.", NUM_MSGS, msgCount, repliedCount, lastRepliedId);
                    }
                    else if(errString != null)
                    {
                        Assert.Fail("Asynchronous failure occurred. Cause : {0}", errString);
                    }
                    else
                    {
                        Assert.IsTrue(replierFinished.await(TimeSpan.FromMilliseconds(timeout)), "Replier thread has not finished sending messages. Remaining {0}", replierFinished.Remaining);
                        Assert.IsNull(asyncEx, "Received Exception Asynchronously. Cause : {0}", asyncEx);
                        Assert.AreEqual(NUM_MSGS, msgCount, "Failed to receive all messages.");
                        Assert.AreEqual(NUM_MSGS, repliedCount, "Failed to reply to all messages");
                        Assert.AreEqual(NUM_MSGS - 1, lastRepliedId, "Failed to receive the final message");
                    }

                }
                catch (Exception ex)
                {
                    this.PrintTestFailureAndAssert(GetTestMethodName(), "Unexpected exception.", ex);
                }
                
            }
        }

        private int DrainDestination(
            ISession consumerFactory,
            IDestination destination,
            int msgsToDrain = -1,
            int timeout = TIMEOUT
            )
        {
            using (IMessageConsumer drain = consumerFactory.CreateConsumer(destination))
            {
                return DrainDestination(drain, destination, msgsToDrain, timeout);
            }
        }

        private int DrainDestination(IMessageConsumer drain, IDestination destination, int msgsToDrain = -1, int timeout = TIMEOUT )
        {
            int msgsDrained = 0;
            try
            {
                if (msgsToDrain > 0)
                {
                    while (msgsToDrain > msgsDrained)
                    {
                        IMessage msg = drain.Receive(TimeSpan.FromMilliseconds(timeout));
                        if (msg == null) break;
                        msgsDrained++;
                    }
                }
                else
                {
                    IMessage msg = null;
                    while ((msg = drain.Receive(TimeSpan.FromMilliseconds(timeout))) != null)
                    {
                        msgsDrained++;
                    }
                }
                
            }
            catch(Exception ex)
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("Failed to drain Destination {0}", destination.ToString());
                if (msgsToDrain != -1)
                {
                    sb.AppendFormat(", Drained {0} of {1} messages", msgsDrained, msgsToDrain);
                }
                else
                {
                    sb.AppendFormat(", Drained {0} messages", msgsDrained);
                }
                if (timeout > 0)
                {
                    sb.AppendFormat(" in {0}ms", timeout);
                }

                throw new Exception(sb.ToString(), ex);
            }
            return msgsDrained;
        }

        [Test]
        [ConnectionSetup(null, "default")]
        [SessionSetup("default", "s1")]
        [SkipTestOnRemoteBrokerProperties("default", RemotePlatform = NMSTestConstants.NMS_SOLACE_PLATFORM)]
        public void TestCreateTemporaryDestination()
        {
            const int NUM_MSGS = 10;
            try
            {


                using (IConnection connection = GetConnection("default"))
                using (ISession session = GetSession("s1"))
                {
                    IStreamMessage msg = session.CreateStreamMessage();
                    
                    IDestination temp = session.CreateTemporaryQueue();
                    
                    IMessageProducer producer = session.CreateProducer(temp);
                    
                    for (int i = 0; i < NUM_MSGS; i++)
                    {
                        msg.WriteObject("barfoo");
                        msg.WriteObject(i);

                        msg.Properties.SetInt("count", i);

                        producer.Send(msg);

                        msg.ClearBody();
                    }

                    // Queues do not require an active consumer to receive messages.
                    // Create consumer on queue after messages sent and receive messages.
                    IMessageConsumer drain = session.CreateConsumer(temp);

                    connection.Start();

                    int msgsReceived = DrainDestination(drain,  temp, NUM_MSGS);

                    Assert.AreEqual(NUM_MSGS, msgsReceived, "Received {0} of {1} on temporary destination {2}.", msgsReceived, NUM_MSGS, temp.ToString());

                    temp = session.CreateTemporaryTopic();

                    // Topics require an active consumer to receive messages.
                    drain = session.CreateConsumer(temp);

                    producer = session.CreateProducer(temp);

                    for (int i = 0; i < NUM_MSGS; i++)
                    {
                        msg.WriteObject("foobar");
                        msg.WriteObject(i);

                        msg.Properties.SetInt("count", i);

                        producer.Send(msg);

                        msg.ClearBody();
                    }
                    
                    msgsReceived = DrainDestination(drain, temp, NUM_MSGS);

                    Assert.AreEqual(NUM_MSGS, msgsReceived, "Received {0} of {1} on temporary destination {2}.", msgsReceived, NUM_MSGS, temp.ToString());
                    
                }
            }
            catch (Exception ex)
            {
                this.PrintTestFailureAndAssert(GetTestMethodName(), "Unexpected exception.", ex);
            }
        }

        [Test]
        [ConnectionSetup(null, "c1")]
        [SessionSetup("c1", "s1", "s2", "tFactory")]
        [TemporaryTopicSetup("tFactory", "temp")]
        [ProducerSetup("s2", "temp", "sender", DeliveryMode = MsgDeliveryMode.Persistent)]
        [ConsumerSetup("s1", "temp", "receiver")]
        public void TestSendToTemporaryOnClosedSession()
        {
            const int NUM_MSGS = 100;
            string errString = null;
            const int TIMEOUT = NUM_MSGS * 100;
            try
            {
                using (IConnection connection = GetConnection("c1"))
                using (ISession tFactory = GetSession("tFactory"))
                using (IMessageProducer producer = GetProducer("sender"))
                using (IMessageConsumer consumer = GetConsumer("receiver"))
                {
                    IDestination destination = GetDestination("temp");
                    ITextMessage sendMessage = producer.CreateTextMessage();
                    MessageListener ackCallback = CreateListener(NUM_MSGS);
                    MessageListener callback = (message) =>
                    {
                        if (errString == null)
                        {

                            if (!destination.Equals(message.NMSReplyTo))
                            {
                                errString = string.Format("Received message, id = {0}, has incorrect ReplyTo property.", ExtractMsgId(message.NMSMessageId));
                                waiter.Set();
                            }

                            ackCallback(message);
                        }
                    };
                    consumer.Listener += callback;
                    connection.ExceptionListener += DefaultExceptionListener;

                    sendMessage.NMSReplyTo = destination;

                    connection.Start();

                    // close session
                    tFactory.Close();

                    for(int i = 0; i < NUM_MSGS; i++)
                    {
                        sendMessage.Text = string.Format("Link:{0},count:{1}", "temp", i);
                        producer.Send(sendMessage);

                        sendMessage.ClearBody();
                    }

                    if (!waiter.WaitOne(TIMEOUT))
                    {
                        if(errString == null)
                        {
                            Assert.Fail("Timed out waiting messages. Received, {0} of {1} messages in {2}ms.", msgCount, NUM_MSGS, TIMEOUT);
                        }
                        else
                        {
                            Assert.Fail(errString);
                        }
                    }

                    Assert.AreEqual(NUM_MSGS, msgCount, "Did not receive expected number of messages.");
                }
            }
            catch(Exception ex)
            {
                this.PrintTestFailureAndAssert(GetTestMethodName(), "Unexpected exception.", ex);
            }
        }

        #endregion // end Temporary Destination tests

        #endregion // end Destination tests
    }
}
