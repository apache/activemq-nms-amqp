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
using System.Collections.Specialized;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using Apache.NMS;
using Apache.NMS.AMQP.Test.Util;
using Apache.NMS.AMQP.Test.Attribute;

namespace Apache.NMS.AMQP.Test.TestCase
{
    [TestFixture]
    class SessionTest : BaseTestCase
    {

        public override void Setup()
        {
            //Console.WriteLine("Test {0}", TestContext.CurrentContext.Test.Name);
            base.Setup();
            msgCount = 0;
            asyncEx = null;
            waiter = new System.Threading.ManualResetEvent(false);
        }

        [Test]
        [ConnectionSetup(null,"default", ClientId = "ID:foobartest")]
        public void TestSessionStart()
        {
            try
            {


                IConnection conn = this.GetConnection("default");

                ISession session = conn.CreateSession();
                
                ISession session1 = conn.CreateSession();

                Logger.Info(string.Format("Starting connection {0}", conn.ClientId));
                conn.Start();
                
                Logger.Info(string.Format("Closing connection {0}", conn.ClientId));
                conn.Close();
                Logger.Info(string.Format("Closed connection {0}", conn.ClientId));
            }
            catch (Exception ex)
            {
                this.PrintTestFailureAndAssert(this.GetMethodName(), "Unexpected Exception raised.", ex);
            }
        }

        [Test]
        [ConnectionSetup(null, "default")]
        [SessionSetup("default", "default")]
        public void TestSessionThrowIfConnectionClosed()
        {
            using (IConnection conn = GetConnection())
            using (ISession session = GetSession("default"))
            {
                conn.Start();
                IMessage msg = session.CreateMessage();
                try
                {
                    conn.Close();
                    msg = session.CreateMessage();
                    Assert.Fail("Should Throw NMSException for Closed Session.");
                }
                catch(NMSException ne)
                {
                    Assert.True(ne is IllegalStateException, "Didn't receive Correct NMSException for Operation on closed Session.");
                    Assert.AreEqual("Invalid Operation on Closed session.", ne.Message);
                }
                catch (Exception e)
                {
                    PrintTestFailureAndAssert(GetMethodName(), "Expected Excepted Exception.", e);
                }
                finally
                {
                    session?.Dispose();
                    conn?.Dispose();
                }
                

            }
        }

        [Test]
        [ConnectionSetup(null, "default")]
        [SessionSetup("default", "default")]
        public void TestSessionThrowIfSessionClosed()
        {
            using (IConnection conn = GetConnection())
            using (ISession session = GetSession("default"))
            {
                conn.Start();
                IMessage msg = session.CreateMessage();

                try
                {
                    session.Close();
                    msg = session.CreateMessage();
                    Assert.Fail("Should Throw NMSException for Closed Session.");
                }
                catch (NMSException ne)
                {
                    Assert.True(ne is IllegalStateException, "Didn't receive Correct NMSException for Operation on closed Session.");
                    Assert.AreEqual("Invalid Operation on Closed session.", ne.Message);
                }
                catch (Exception e)
                {
                    PrintTestFailureAndAssert(GetMethodName(), "Expected Excepted Exception.", e);
                }
                finally
                {
                    session?.Dispose();
                    conn?.Dispose();
                }


            }

        }

        [Test]
        [ConnectionSetup(null, "default")]
        [SessionSetup("default", "default", AckMode = AcknowledgementMode.ClientAcknowledge)]
        [TopicSetup("default", "t1", Name = "nms.topic.test")]
        [ProducerSetup("default", "t1", "p1", DeliveryMode = MsgDeliveryMode.NonPersistent)]
        [ConsumerSetup("default", "t1", "c1")]
        public void TestSessionRecoverOnDispatchThread()
        {
            const int NUM_MGS = 100;
            const int RECOVER_MSGS = 10;
            const int TOTAL_MSGS = NUM_MGS + RECOVER_MSGS;
            int TIMEOUT = Math.Max((TOTAL_MSGS / 1000), 1) * 1000 * 3;
            int recoveredMessageCount = 0;

            using (IConnection connection = GetConnection("default"))
            using (ISession session = GetSession("default"))
            using (IMessageProducer producer = GetProducer("p1"))
            using (IMessageConsumer consumer = GetConsumer("c1"))
            {
                try
                {
                    MessageListener countCallback = CreateListener(TOTAL_MSGS);
                    MessageListener callback = (m) =>
                    {
                        if (TOTAL_MSGS / 2 == msgCount)
                        {
                            m.Acknowledge();
                        }
                        else if (msgCount == TOTAL_MSGS - 1)
                        {
                            m.Acknowledge();
                        }
                        if (m.NMSRedelivered)
                        {
                            recoveredMessageCount++;
                        }

                        countCallback(m);
                        if(msgCount == RECOVER_MSGS)
                        {
                            session.Recover();
                        }
                        
                        
                    };

                    consumer.Listener += callback;
                    connection.ExceptionListener += DefaultExceptionListener;

                    ITextMessage sendMessage = session.CreateTextMessage();

                    connection.Start();

                    for(int i=0; i<NUM_MGS; i++)
                    {
                        sendMessage.Text = "Num:" + i;
                        producer.Send(sendMessage);
                    }


                    if (!waiter.WaitOne(TIMEOUT))
                    {
                        Assert.IsNull(asyncEx, "Asynchronously received exception {0} with failing to receive {1} of {2} in {3}ms", asyncEx?.Message, msgCount, TOTAL_MSGS, TIMEOUT);
                        Assert.Fail("Failed to receive {0} of {1} in {2}ms", msgCount, TOTAL_MSGS, TIMEOUT);
                    }
                    Assert.IsNull(asyncEx, "Asynchronously received exception {0}", asyncEx?.Message);
                    Assert.AreEqual(RECOVER_MSGS, recoveredMessageCount, "Did not recover expected Messages.");
                    Assert.AreEqual(TOTAL_MSGS, msgCount, "Did not receive expected messages.");

                }
                catch (Exception ex)
                {
                    this.PrintTestFailureAndAssert(GetTestMethodName(), "Unexpected Exception.", ex);
                }
            }

        }


    }
}
