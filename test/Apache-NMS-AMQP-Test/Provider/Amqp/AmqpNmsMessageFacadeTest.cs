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
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS;
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.AMQP.Provider.Amqp;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Apache.NMS.AMQP.Util;
using NUnit.Framework;

namespace NMS.AMQP.Test.Provider.Amqp
{
    [TestFixture]
    public class AmqpNmsMessageFacadeTest : AmqpNmsMessageTypesTestCase
    {
        /// <summary>
        /// To satisfy the JMS requirement that messages are durable by default, the
        /// AmqpNmsMessageFacade objects created for sending new messages are
        /// populated with a header section with durable set to true.
        /// </summary>
        [Test]
        public void TestNewMessageHasUnderlyingHeaderSectionWithDurableTrue()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.NotNull(amqpNmsMessageFacade.Header, "Expected message to have Header section");
            Assert.True(amqpNmsMessageFacade.Header.Durable, "Durable not as expected");
        }

        // --- ttl field  ---

        [Test, Ignore("Confirm - it should be zero")]
        public void TestNewMessageHasUnderlyingHeaderSectionWithNoTtlSet()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.NotNull(amqpNmsMessageFacade.Header, "Expected message to have Header section");
            Assert.AreEqual(default(uint), amqpNmsMessageFacade.Header.Ttl, "Ttl field should not be set");
        }

        [Test]
        public void TestSetGetTtlOverrideOnNewMessage()
        {
            uint origTtl = 5;
            global::Amqp.Message message = new global::Amqp.Message {Header = new Header {Ttl = origTtl}};

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            Assert.AreEqual(origTtl, message.Header.Ttl, "TTL has been unset already");

            amqpNmsMessageFacade.OnSend(NMSConstants.defaultTimeToLive);

            Assert.AreEqual(origTtl, message.Header.Ttl, "TTL has been overriden");
        }

        [Test]
        public void TestOnSendOverridesTtlOnMessageReceivedWithTtl()
        {
            uint origTtl = 5;
            uint newTtl = 10;
            global::Amqp.Message message = new global::Amqp.Message {Header = new Header {Ttl = origTtl}};

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            Assert.AreEqual(origTtl, message.Header.Ttl, "TTL has been unset already");

            amqpNmsMessageFacade.OnSend(TimeSpan.FromMilliseconds(newTtl));

            Assert.AreEqual(newTtl, message.Header.Ttl, "TTL has not been overriden");
        }

        [Test]
        public void TestOnSendOverridesProviderTtlWithSpecifiedOverrideTtl()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            uint overrideTtl = 5;
            uint producerTtl = 10;

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);
            amqpNmsMessageFacade.NMSTimeToLive = TimeSpan.FromMilliseconds(overrideTtl);

            amqpNmsMessageFacade.OnSend(TimeSpan.FromMilliseconds(producerTtl));

            // check value on underlying TTL field is set to the override
            Assert.AreEqual(overrideTtl, message.Header.Ttl, "TTL has not been overriden");
        }

        // --- delivery count  ---

        [Test]
        public void TestGetDeliveryCountIs1ForNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            // JMS delivery count starts at one.
            Assert.AreEqual(1, amqpNmsMessageFacade.DeliveryCount);

            // Redelivered state inferred from delivery count
            Assert.IsFalse(amqpNmsMessageFacade.NMSRedelivered);
            Assert.AreEqual(0, amqpNmsMessageFacade.RedeliveryCount);
        }

        [Test]
        public void TestGetDeliveryCountForReceivedMessageWithNoHeader()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            // JMS delivery count starts at one.
            Assert.AreEqual(1, amqpNmsMessageFacade.DeliveryCount);

            // Redelivered state inferred from delivery count
            Assert.IsFalse(amqpNmsMessageFacade.NMSRedelivered);
            Assert.AreEqual(0, amqpNmsMessageFacade.RedeliveryCount);
        }

        [Test]
        public void TestGetDeliveryCountForReceivedMessageWithHeaderButNoDeliveryCount()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                Header = new Header()
            };
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            // JMS delivery count starts at one.
            Assert.AreEqual(1, amqpNmsMessageFacade.DeliveryCount);

            // Redelivered state inferred from delivery count
            Assert.IsFalse(amqpNmsMessageFacade.NMSRedelivered);
            Assert.AreEqual(0, amqpNmsMessageFacade.RedeliveryCount);
        }

        [Test]
        public void TestGetDeliveryCountForReceivedMessageWithHeaderWithDeliveryCount()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                Header = new Header {DeliveryCount = 1}
            };
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            // JMS delivery count starts at one.
            Assert.AreEqual(2, amqpNmsMessageFacade.DeliveryCount);

            // Redelivered state inferred from delivery count
            Assert.True(amqpNmsMessageFacade.NMSRedelivered);
            Assert.AreEqual(1, amqpNmsMessageFacade.RedeliveryCount);
        }

        [Test]
        public void TestSetRedeliveredAltersDeliveryCount()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            Assert.False(amqpNmsMessageFacade.NMSRedelivered);
            Assert.AreEqual(0, amqpNmsMessageFacade.RedeliveryCount);

            amqpNmsMessageFacade.NMSRedelivered = true;
            Assert.True(amqpNmsMessageFacade.NMSRedelivered);
            Assert.AreEqual(1, amqpNmsMessageFacade.RedeliveryCount);
        }

        [Test]
        public void TestSetRedeliveredWhenAlreadyRedeliveredDoesNotChangeDeliveryCount()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                Header = new Header {DeliveryCount = 1}
            };
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            // Redelivered state inferred from delivery count
            Assert.True(amqpNmsMessageFacade.NMSRedelivered);
            Assert.AreEqual(1, amqpNmsMessageFacade.RedeliveryCount);

            amqpNmsMessageFacade.NMSRedelivered = true;
            Assert.True(amqpNmsMessageFacade.NMSRedelivered);
            Assert.AreEqual(1, amqpNmsMessageFacade.RedeliveryCount);
        }

        [Test]
        public void TestSetRedeliveredFalseClearsDeliveryCount()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                Header = new Header {DeliveryCount = 1}
            };
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            // Redelivered state inferred from delivery count
            Assert.True(amqpNmsMessageFacade.NMSRedelivered);
            Assert.AreEqual(1, amqpNmsMessageFacade.RedeliveryCount);

            amqpNmsMessageFacade.NMSRedelivered = false;
            Assert.False(amqpNmsMessageFacade.NMSRedelivered);
            Assert.AreEqual(0, amqpNmsMessageFacade.RedeliveryCount);
        }

        [Test]
        public void TestSetRedeliveryCountToZeroWhenNoHeadersNoNPE()
        {
            global::Amqp.Message message = new global::Amqp.Message();

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);
            amqpNmsMessageFacade.RedeliveryCount = 0;
        }

        // --- priority field  ---
        [Test]
        public void TestGetPriorityIs4ForNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            Assert.AreEqual(MsgPriority.BelowNormal, amqpNmsMessageFacade.NMSPriority, "expected priority value not found");
        }

        // When messages have no header section, the AMQP spec says the priority has default value of 4."
        [Test]
        public void TestGetPriorityIs4ForReceivedMessageWithNoHeader()
        {
            global::Amqp.Message message = new global::Amqp.Message();

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);
            Assert.AreEqual(MsgPriority.BelowNormal, amqpNmsMessageFacade.NMSPriority, "expected priority value not found");
        }

        // When messages have a header section, but lack the priority field, the AMQP spec says the priority has default value of 4.
        [Test]
        public void TestGetPriorityIs4ForReceivedMessageWithHeaderButWithoutPriority()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                Header = new Header()
            };

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);
            Assert.AreEqual(MsgPriority.BelowNormal, amqpNmsMessageFacade.NMSPriority, "expected priority value not found");
        }

        // "When messages have a header section, which have a priority value, ensure it is returned."
        [Test]
        public void TestGetPriorityForReceivedMessageWithHeaderWithPriority()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                Header = new Header {Priority = 7}
            };

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            Assert.AreEqual(MsgPriority.High, amqpNmsMessageFacade.NMSPriority);
        }

        // When messages have a header section, which has a priority value just above the
        // JMS range of 0-9, ensure it is constrained to 9.
        [Test]
        public void TestGetPriorityForReceivedMessageWithPriorityJustAboveJmsRange()
        {
            // value just over 9 deliberately
            byte priority = 11;

            global::Amqp.Message message = new global::Amqp.Message
            {
                Header = new Header {Priority = priority}
            };

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            Assert.AreEqual(MsgPriority.Highest, amqpNmsMessageFacade.NMSPriority);
        }

        // Test that setting the Priority to a non-default value results in the underlying
        // message field being populated appropriately, and the value being returned from the Getter.
        [Test]
        public void TestSetGetNonDefaultPriorityForNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            amqpNmsMessageFacade.NMSPriority = MsgPriority.AboveNormal;

            Assert.AreEqual(MsgPriority.AboveNormal, amqpNmsMessageFacade.NMSPriority);
            Assert.AreEqual(6, amqpNmsMessageFacade.Header.Priority);
        }

        // Test that setting the Priority above the JMS range of 0-9 results in the underlying
        // message field being populated with the value 9.
        [Test]
        public void TestSetPriorityAboveJmsRangeForNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.NMSPriority = (MsgPriority) 11;

            Assert.AreEqual(MsgPriority.Highest, amqpNmsMessageFacade.NMSPriority);
            Assert.AreEqual(9, amqpNmsMessageFacade.Header.Priority);
        }

        // ====== AMQP Properties Section =======
        // ======================================

        [Test]
        public void TestGetGroupIdIsNullForNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.IsNull(amqpNmsMessageFacade.GroupId);
        }

        /*
         * Check that setting GroupId null on a new message does not cause creation of the underlying properties
         * section. New messages lack the properties section. 
         */
        [Test]
        public void TestSetGroupIdNullOnNewMessageDoesNotCreatePropertiesSection()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.GroupId = null;

            Assert.Null(amqpNmsMessageFacade.Message.Properties, "properties section was created");
        }

        /*
         * Check that setting GroupId on the message causes creation of the underlying properties
         * section with the expected value.
         */
        [Test]
        public void TestSetGroupIdOnNewMessage()
        {
            string groupId = "testValue";
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.GroupId = groupId;

            Assert.NotNull(amqpNmsMessageFacade.Message.Properties, "properties section was not created");
            Assert.AreEqual(groupId, amqpNmsMessageFacade.Message.Properties.GroupId);
            Assert.AreEqual(groupId, amqpNmsMessageFacade.GroupId);
        }

        [Test]
        public void TestSetGroupIdNullOnMessageWithExistingGroupId()
        {
            string groupId = "testValue";
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            amqpNmsMessageFacade.GroupId = groupId;
            amqpNmsMessageFacade.GroupId = null;

            Assert.Null(amqpNmsMessageFacade.Message.Properties.GroupId, "value was not cleared for GroupId as expected");
            Assert.Null(amqpNmsMessageFacade.GroupId, "value was not cleared for GroupId as expected");
        }

        // --- reply-to-group-id field ---

        /*
         * Check that setting the ReplyToGroupId works on new messages without a properties
         * properties section. New messages lack the properties section.
         */
        [Test]
        public void TestGetReplyToGroupIdIsNullForNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.IsNull(amqpNmsMessageFacade.ReplyToGroupId);
        }

        /*
         * Check that getting the ReplyToGroupId works on received messages without a properties section
         */
        [Test]
        public void TestGetReplyToGroupIdWithReceivedMessageWithNoProperties()
        {
            global::Amqp.Message message = new global::Amqp.Message();

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            Assert.Null(amqpNmsMessageFacade.ReplyToGroupId);
        }

        [Test]
        public void TestSetReplyToGroupIdNullOnNewMessageDoesNotCreatePropertiesSection()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            amqpNmsMessageFacade.ReplyToGroupId = null;

            Assert.Null(amqpNmsMessageFacade.Message.Properties, "properties section was created");
        }

        [Test]
        public void TestGetReplyToGroupIdWithReceivedMessageWithPropertiesButNoReplyToGroupId()
        {
            global::Amqp.Message message = new global::Amqp.Message()
            {
                Properties = new Properties()
            };

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);
            Assert.Null(amqpNmsMessageFacade.ReplyToGroupId,
                "expected ReplyToGroupId to be null on message with properties section but no reply-to-group-id");
        }

        [Test]
        public void TestGetReplyToGroupIdWithReceivedMessage()
        {
            string replyToGroupId = "myReplyGroup";
            global::Amqp.Message message = new global::Amqp.Message()
            {
                Properties = new Properties()
                {
                    ReplyToGroupId = replyToGroupId
                }
            };

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);
            Assert.AreEqual(replyToGroupId, amqpNmsMessageFacade.ReplyToGroupId, "expected ReplyToGroupId on message was not found");
        }

        [Test]
        public void TestSetReplyToGroupId()
        {
            string replyToGroupId = "myReplyGroup";

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.ReplyToGroupId = replyToGroupId;

            Assert.AreEqual(replyToGroupId, amqpNmsMessageFacade.Message.Properties.ReplyToGroupId,
                "expected ReplyToGroupId on message was not found");
        }

        [Test]
        public void TestSetReplyToGroupIdNullClearsProperty()
        {
            string replyToGroupId = "myReplyGroup";

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.ReplyToGroupId = replyToGroupId;

            Assert.AreEqual(replyToGroupId, amqpNmsMessageFacade.Message.Properties.ReplyToGroupId,
                "expected ReplyToGroupId on message was not found");

            amqpNmsMessageFacade.ReplyToGroupId = null;
            Assert.Null(amqpNmsMessageFacade.ReplyToGroupId, "expected ReplyToGroupId on message to be null");
        }

        [Test]
        public void TestSetGetReplyToGroupId()
        {
            string replyToGroupId = "myReplyGroup";

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.Null(amqpNmsMessageFacade.ReplyToGroupId);

            amqpNmsMessageFacade.ReplyToGroupId = replyToGroupId;

            Assert.AreEqual(replyToGroupId, amqpNmsMessageFacade.Message.Properties.ReplyToGroupId,
                "expected ReplyToGroupId on message was not found");
        }

        // --- group-sequence field ---

        [Test]
        public void TestSetGetGroupSequenceOnNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            uint groupSequence = 5;
            amqpNmsMessageFacade.GroupSequence = groupSequence;

            Assert.AreEqual(groupSequence, amqpNmsMessageFacade.Message.Properties.GroupSequence, "GroupSequence not as expected");
            Assert.AreEqual(groupSequence, amqpNmsMessageFacade.GroupSequence, "GroupSequence not as expected");
        }

        [Test]
        public void TestClearGroupSequenceOnMessageWithoutExistingGroupSequence()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            amqpNmsMessageFacade.GroupSequence = 0;

            Assert.Null(amqpNmsMessageFacade.Message.Properties, "underlying message should still have no properties section");
            Assert.AreEqual(0, amqpNmsMessageFacade.GroupSequence, "GroupSequence should be 0");
        }

        // --- to field ---

        [Test]
        public void TestSetDestinationWithNullClearsProperty()
        {
            string testToAddress = "myTestAddress";
            NmsTopic destination = new NmsTopic(testToAddress);

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            amqpNmsMessageFacade.NMSDestination = null;
            Assert.Null(amqpNmsMessageFacade.Message.Properties);
            amqpNmsMessageFacade.NMSDestination = destination;
            Assert.NotNull(amqpNmsMessageFacade.Message.Properties);
            Assert.NotNull(amqpNmsMessageFacade.Message.Properties.To);

            amqpNmsMessageFacade.NMSDestination = null;
            Assert.Null(amqpNmsMessageFacade.Message.Properties.To);
        }

        [Test]
        public void TestSetGetDestination()
        {
            string testToAddress = "myTestAddress";
            NmsTopic destination = new NmsTopic(testToAddress);

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.Null(amqpNmsMessageFacade.Message.Properties);

            amqpNmsMessageFacade.NMSDestination = destination;

            Assert.NotNull(amqpNmsMessageFacade.Properties);
            Assert.AreEqual(testToAddress, amqpNmsMessageFacade.Message.Properties.To);
            Assert.AreEqual(destination, amqpNmsMessageFacade.NMSDestination);
        }

        [Test]
        public void TestGetDestinationWithReceivedMessage()
        {
            string testToAddress = "myTestAddress";
            global::Amqp.Message message = new global::Amqp.Message()
            {
                Properties = new Properties {To = testToAddress}
            };

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            //We didn't set any destination type annotations, so the consumer destination type will be used: a topic.
            Assert.IsInstanceOf<ITopic>(amqpNmsMessageFacade.NMSDestination);
            Assert.AreEqual(testToAddress, ((ITopic) amqpNmsMessageFacade.NMSDestination).TopicName);
        }

        [Test]
        public void TestGetDestinationWithReceivedMessageWithoutPropertiesUsesConsumerDestination()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            IAmqpConsumer consumer = CreateMockConsumer();
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(consumer, message);

            Assert.NotNull(amqpNmsMessageFacade.NMSDestination);
            Assert.AreEqual(consumer.Destination, amqpNmsMessageFacade.NMSDestination);
        }

        // --- reply-to field ---

        [Test]
        public void TestSetGetReplyToWithNullClearsProperty()
        {
            string testReplyToAddress = "myTestReplyTo";
            NmsTopic dest = new NmsTopic(testReplyToAddress);

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.IsNull(amqpNmsMessageFacade.Message.Properties);
            amqpNmsMessageFacade.NMSReplyTo = dest;

            Assert.IsNotNull(amqpNmsMessageFacade.Properties);
            Assert.NotNull(amqpNmsMessageFacade.Message.Properties.ReplyTo);
        }

        [Test]
        public void TestSetGetReplyTo()
        {
            string testReplyToAddress = "myTestReplyTo";
            NmsTopic dest = new NmsTopic(testReplyToAddress);

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.IsNull(amqpNmsMessageFacade.Message.Properties);
            amqpNmsMessageFacade.NMSReplyTo = dest;

            Assert.IsNotNull(amqpNmsMessageFacade.Properties);
            Assert.AreEqual(testReplyToAddress, amqpNmsMessageFacade.Message.Properties.ReplyTo);
            Assert.AreEqual(dest, amqpNmsMessageFacade.NMSReplyTo);
        }

        [Test]
        public void TestGetReplyToWithReceivedMessage()
        {
            string testReplyToAddress = "myTestReplyTo";
            global::Amqp.Message message = new global::Amqp.Message()
            {
                Properties = new Properties()
                {
                    ReplyTo = testReplyToAddress
                }
            };

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            //We didn't set any destination type annotations, so the consumer destination type will be used: a topic.
            Assert.IsInstanceOf<ITopic>(amqpNmsMessageFacade.NMSReplyTo);
            Assert.AreEqual(testReplyToAddress, ((ITopic) amqpNmsMessageFacade.NMSReplyTo).TopicName);
        }

        [Test]
        public void TestGetReplyToWithReceivedMessageWithoutProperties()
        {
            global::Amqp.Message message = new global::Amqp.Message();
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);
            Assert.Null(amqpNmsMessageFacade.NMSReplyTo);
        }

        // --- message-id and correlation-id ---

        [Test]
        public void TestGetCorrelationIdIsNullOnNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.Null(amqpNmsMessageFacade.NMSCorrelationID);
        }

        /*
         * Test that setting then getting an application-specific String as the CorrelationId returns
         * the expected value and sets the expected value on the underlying AMQP message.
         */
        [Test]
        public void TestSetGetCorrelationIdOnNewMessageWithStringAppSpecific()
        {
            string testCorrelationId = "myAppSpecificStringCorrelationId";

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.NMSCorrelationID = testCorrelationId;

            Assert.AreEqual(testCorrelationId, amqpNmsMessageFacade.Message.Properties.CorrelationId,
                "correlationId value on underlying AMQP message not as expected");
            Assert.AreEqual(testCorrelationId, amqpNmsMessageFacade.NMSCorrelationID, "Expected correlationId not returned");
        }

        /*
         * Test that setting then getting an JMSMessageID String as the CorrelationId returns
         * the expected value and sets the expected value on the underlying AMQP message
         */
        [Test]
        public void TestSetGetCorrelationIdOnNewMessageWithStringJmsMessageId()
        {
            string testCorrelationId = "ID:myJMSMessageIDStringCorrelationId";

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.NMSCorrelationID = testCorrelationId;

            Assert.AreEqual(testCorrelationId, amqpNmsMessageFacade.Message.Properties.GetCorrelationId(),
                "correlationId value on underlying AMQP message not as expected");
            Assert.AreEqual(testCorrelationId, amqpNmsMessageFacade.NMSCorrelationID, "Expected correlationId not returned from facade");
        }

        /*
         * Test that setting the correlationId null, clears an existing value in the
         * underlying AMQP message correlation-id field
         */
        [Test]
        public void TestSetCorrelationIdNullClearsExistingValue()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.NMSCorrelationID = "cid";
            amqpNmsMessageFacade.NMSCorrelationID = null;

            Assert.IsNull(amqpNmsMessageFacade.Message.Properties.GetCorrelationId(), "Unexpected correlationId value on underlying AMQP message");
            Assert.IsNull(amqpNmsMessageFacade.NMSCorrelationID, "Expected correlationId bytes to be null");
        }

        /*
         * Test that getting the correlationId when using an underlying received message with
         * an application-specific (no 'ID:' prefix) String correlation id returns the expected value.
         */
        [Test]
        public void TestGetCorrelationIdOnReceivedMessageWithStringAppSpecific()
        {
            string testCorrelationId = "myCorrelationIdString";
            CorrelationIdOnReceivedMessageTestImpl(testCorrelationId, testCorrelationId, true);
        }

        /*
         * Test that getting the correlationId when using an underlying received message with
         * a String correlation id representing a JMSMessageID (i.e there is an ID: prefix)
         * returns the expected value.
         */
        [Test]
        public void TestGetCorrelationIdOnReceivedMessageWithStringJmsMessageId()
        {
            string testCorrelationId = "ID:JMSMessageIDasCorrelationIdString";
            CorrelationIdOnReceivedMessageTestImpl(testCorrelationId, testCorrelationId, false);
        }

        /*
         * Test that setting then getting a UUID as the correlationId returns the expected value,
         * and sets the expected value on the underlying AMQP message.
         */
        [Test]
        public void TestSetGetCorrelationIdOnNewMessageWithUuid()
        {
            Guid testCorrelationId = Guid.NewGuid();
            string converted = "ID:AMQP_UUID:" + testCorrelationId;

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.NMSCorrelationID = converted;

            Assert.AreEqual(testCorrelationId, amqpNmsMessageFacade.Message.Properties.GetCorrelationId());
            Assert.AreEqual(converted, amqpNmsMessageFacade.NMSCorrelationID);
        }

        /*
         * Test that getting the correlationId when using an underlying received message with a
         * UUID correlation id returns the expected value.
         */
        [Test]
        public void TestGetCorrelationIdOnReceivedMessageWithUuid()
        {
            Guid testCorrelationId = Guid.NewGuid();
            string expected = AmqpMessageIdHelper.NMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_UUID_PREFIX + testCorrelationId;

            CorrelationIdOnReceivedMessageTestImpl(testCorrelationId, expected, false);
        }

        /*
         * Test that setting then getting a ulong correlationId (using BigInteger) returns the expected value
         * and sets the expected value on the underlying AMQP message
         */
        [Test]
        public void TestSetGetCorrelationIdOnNewMessageWithUnsignedLong()
        {
            object testCorrelationId = 123456789L;
            string converted = "ID:AMQP_ULONG:" + testCorrelationId;

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.NMSCorrelationID = converted;

            Assert.AreEqual(testCorrelationId, amqpNmsMessageFacade.Message.Properties.GetCorrelationId());
            Assert.AreEqual(converted, amqpNmsMessageFacade.NMSCorrelationID);
        }

        /*
         * Test that getting the correlationId when using an underlying received message with a
         * ulong correlation id (using BigInteger) returns the expected value.
         */
        [Test]
        public void TestGetCorrelationIdOnReceivedMessageWithUnsignedLong()
        {
            object testCorrelationId = 123456789ul;
            string expected = AmqpMessageIdHelper.NMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_ULONG_PREFIX + testCorrelationId.ToString();
            CorrelationIdOnReceivedMessageTestImpl(testCorrelationId, expected, false);
        }

        /*
         * Test that setting then getting binary as the correlationId returns the expected value
         * and sets the correlation id field as expected on the underlying AMQP message
         */
        [Test]
        public void TestSetGetCorrelationIdOnNewMessageWithBinary()
        {
            byte[] testCorrelationId = CreateBinaryId();

            string converted = "ID:AMQP_BINARY:" + AmqpMessageIdHelper.ConvertBinaryToHexString(testCorrelationId);

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.NMSCorrelationID = converted;

            Assert.AreEqual(testCorrelationId, amqpNmsMessageFacade.Message.Properties.GetCorrelationId());
            Assert.AreEqual(converted, amqpNmsMessageFacade.NMSCorrelationID);
        }

        /*
         * Test that getting the correlationId when using an underlying received message with a
         * Binary message id returns the expected value.
         */
        [Test]
        public void TestGetCorrelationIdOnReceivedMessageWithBinary()
        {
            byte[] testCorrelationId = CreateBinaryId();
            string expected = AmqpMessageIdHelper.NMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX +
                              AmqpMessageIdHelper.ConvertBinaryToHexString(testCorrelationId);
            CorrelationIdOnReceivedMessageTestImpl(testCorrelationId, expected, false);
        }

        private void CorrelationIdOnReceivedMessageTestImpl(object testCorrelationId, string expected, bool appSpecificCorrelationId)
        {
            global::Amqp.Message message = new global::Amqp.Message()
            {
                Properties = new Properties()
            };
            message.Properties.SetCorrelationId(testCorrelationId);

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            string result = amqpNmsMessageFacade.NMSCorrelationID;
            Assert.NotNull(result, "Expected a correlationId on received message");
            Assert.AreEqual(expected, result, "Incorrect correlationId value received");
            if (!appSpecificCorrelationId)
            {
                Assert.True(result.StartsWith(AmqpMessageIdHelper.NMS_ID_PREFIX));
            }
        }

        //--- Message Id field ---

        /*
         * Test that setting then getting a String value as the messageId returns the expected value
         */
        [Test]
        public void TestSetGetMessageIdOnNewMessageWithString()
        {
            string testMessageId = "ID:myStringMessageId";

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            amqpNmsMessageFacade.NMSMessageId = testMessageId;

            Assert.AreEqual(testMessageId, amqpNmsMessageFacade.NMSMessageId);
        }

        /*
         * Test that setting an ID: prefixed JMSMessageId results in the underlying AMQP
         * message holding the value with the ID: prefix retained.
         */
        [Test]
        public void TestSetMessageIdRetainsIdPrefixInUnderlyingMessage()
        {
            string testMessageId = "ID:myStringMessageId";

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            amqpNmsMessageFacade.NMSMessageId = testMessageId;

            Assert.AreEqual(testMessageId, amqpNmsMessageFacade.Message.Properties.MessageId);
        }

        /*
         * Test that setting the messageId null clears a previous value in the
         * underlying amqp message-id field
         */
        [Test]
        public void TestSetMessageIdNullClearsExistingValue()
        {
            string testMessageId = "ID:myStringMessageId";

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.NMSMessageId = testMessageId;

            Assert.IsNotNull(amqpNmsMessageFacade.NMSMessageId);

            amqpNmsMessageFacade.NMSMessageId = null;

            Assert.IsNull(amqpNmsMessageFacade.NMSMessageId);
            Assert.IsNull(amqpNmsMessageFacade.Message.Properties.GetMessageId());
        }

        /*
         * Test that getting the messageId when using an underlying received message with a
         * String message id returns the expected value.
         */
        [Test]
        public void TestGetMessageIdOnReceivedMessageWithString()
        {
            string testMessageId = AmqpMessageIdHelper.NMS_ID_PREFIX + "myMessageIdString";
            MessageIdOnReceivedMessageTestImpl(testMessageId, testMessageId);
        }

        [Test]
        public void TestGetMessageIdOnReceivedMessageWithStringNoIdPrefix()
        {
            //Deliberately omit the "ID:", as if it was sent from a non-JMS client
            object testMessageId = "myMessageIdString";
            string expected = AmqpMessageIdHelper.NMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_NO_PREFIX + testMessageId;
            MessageIdOnReceivedMessageTestImpl(testMessageId, expected);
        }

        /*
         * Test that getting the messageId when using an underlying received message with a
         * UUID message id returns the expected value.
         */
        [Test]
        public void TestGetMessageIdOnReceivedMessageWithUuid()
        {
            Guid testMessageId = Guid.NewGuid();
            string expected = AmqpMessageIdHelper.NMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_UUID_PREFIX + testMessageId;
            MessageIdOnReceivedMessageTestImpl(testMessageId, expected);
        }

        /*
         * Test that getting the messageId when using an underlying received message with a
         * ulong message id returns the expected value.
         */
        [Test]
        public void TestGetMessageIdOnReceivedMessageWithUnsignedLong()
        {
            object testCorrelationId = 123456789ul;
            string expected = AmqpMessageIdHelper.NMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_ULONG_PREFIX + testCorrelationId.ToString();
            MessageIdOnReceivedMessageTestImpl(testCorrelationId, expected);
        }

        /*
         * Test that getting the messageId when using an underlying received message with a
         * Binary message id returns the expected value.
         */
        [Test]
        public void TestGetMessageIdOnReceivedMessageWithBinary()
        {
            byte[] testMessageId = CreateBinaryId();
            string expected = AmqpMessageIdHelper.NMS_ID_PREFIX + AmqpMessageIdHelper.AMQP_BINARY_PREFIX +
                              AmqpMessageIdHelper.ConvertBinaryToHexString(testMessageId);
            MessageIdOnReceivedMessageTestImpl(testMessageId, expected);
        }

        private byte[] CreateBinaryId()
        {
            byte length = 10;
            byte[] idBytes = new byte[length];
            for (int i = 0; i < length; i++)
            {
                idBytes[i] = (byte) (length - i);
            }

            return idBytes;
        }

        private void MessageIdOnReceivedMessageTestImpl(object underlyingMessageId, string expected)
        {
            global::Amqp.Message message = new global::Amqp.Message()
            {
                Properties = new Properties()
            };
            message.Properties.SetMessageId(underlyingMessageId);

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            string result = amqpNmsMessageFacade.NMSMessageId;
            Assert.NotNull(result, "Expected a messageId on received message");
            Assert.AreEqual(expected, result, "Incorrect messageId value received");
        }

        [Test]
        public void TestGetProviderMessageIdObjectOnNewMessage()
        {
            var amqpMessageFacade = CreateNewMessageFacade();
            Assert.IsNull(amqpMessageFacade.ProviderMessageIdObject);
        }

        [Test]
        public void TestSetGetProviderMessageIdObjectOnNewMessageWithString()
        {
            var testMessageId = "ID:myStringMessageId";
            var amqpMessageFacade = CreateNewMessageFacade();
            amqpMessageFacade.ProviderMessageIdObject = testMessageId;
            
            Assert.AreEqual(testMessageId, amqpMessageFacade.ProviderMessageIdObject);
            Assert.AreEqual(testMessageId, amqpMessageFacade.NMSMessageId);
        }

        [Test]
        public void TestSetProviderMessageIdObjectNullClearsProperty()
        {
            var testMessageId = "ID:myStringMessageId";
            var amqpMessageFacade = CreateNewMessageFacade();
            amqpMessageFacade.ProviderMessageIdObject = testMessageId;
            Assert.AreEqual(testMessageId, amqpMessageFacade.ProviderMessageIdObject);

            amqpMessageFacade.ProviderMessageIdObject = null;
            Assert.IsNull(amqpMessageFacade.ProviderMessageIdObject);
        }

        [Test]
        public void TestSetProviderMessageIdObjectNullDoesNotCreateProperties()
        {
            var amqpMessageFacade = CreateNewMessageFacade();
            Assert.IsNull(amqpMessageFacade.Message.Properties);

            amqpMessageFacade.ProviderMessageIdObject = null;
            Assert.IsNull(amqpMessageFacade.Message.Properties);
            Assert.IsNull(amqpMessageFacade.ProviderMessageIdObject);
        }

        // --- creation-time field  ---

        [Test]
        public void TestSetCreationTimeOnNewNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.IsNull(amqpNmsMessageFacade.Message.Properties);

            DateTime expected = DateTime.Now;
            amqpNmsMessageFacade.NMSTimestamp = expected;

            Assert.AreEqual(expected, amqpNmsMessageFacade.NMSTimestamp);
            Assert.NotNull(amqpNmsMessageFacade.Properties);
            Assert.AreEqual(expected, amqpNmsMessageFacade.Message.Properties.CreationTime);
        }

        [Test]
        public void TestGetTimestampIsZeroForNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.AreEqual(default(DateTime), amqpNmsMessageFacade.NMSTimestamp);
        }

        [Test]
        public void TestSetTimestampOnNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            DateTime timestamp = DateTime.Now;

            amqpNmsMessageFacade.NMSTimestamp = timestamp;

            Assert.AreEqual(timestamp, amqpNmsMessageFacade.Message.Properties.CreationTime, "Expected creation-time field to be set");
            Assert.AreEqual(timestamp, amqpNmsMessageFacade.NMSTimestamp, "Expected timestamp");
        }

        [Test]
        public void TestSetTimestampZeroOnNewMessageDoesNotCreatePropertiesSection()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            amqpNmsMessageFacade.NMSTimestamp = default;

            Assert.IsNull(amqpNmsMessageFacade.Message.Properties);
            Assert.AreEqual(default(DateTime), amqpNmsMessageFacade.NMSTimestamp);
        }

        [Test]
        public void TestSetTimestampZeroOnMessageWithExistingTimestampClearsCreationTimeField()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            amqpNmsMessageFacade.NMSTimestamp = DateTime.UtcNow;
            amqpNmsMessageFacade.NMSTimestamp = default;

            Assert.AreEqual(default(DateTime), amqpNmsMessageFacade.Message.Properties.CreationTime);
            Assert.AreEqual(default(DateTime), amqpNmsMessageFacade.NMSTimestamp);
        }

        // --- absolute-expiry-time field  ---
        [Test]
        public void TestGetExpirationIsNullForNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.IsNull(amqpNmsMessageFacade.Expiration);
        }

        [Test]
        public void TestSetGetExpirationOnNewMessage()
        {
            DateTime timestamp = DateTime.UtcNow;

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            amqpNmsMessageFacade.Expiration = timestamp;

            Assert.AreEqual(timestamp, amqpNmsMessageFacade.Message.Properties.AbsoluteExpiryTime, "Expected absolute-expiry-time to be set");
            Assert.AreEqual(timestamp, amqpNmsMessageFacade.Expiration, "Expected expiration to be set");
        }

        [Test]
        public void TestSetExpirationZeroOnNewMessageDoesNotCreatePropertiesSection()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.IsNull(amqpNmsMessageFacade.Message.Properties, "Expected properties section not to exist");

            amqpNmsMessageFacade.Expiration = default;

            Assert.IsNull(amqpNmsMessageFacade.Message.Properties, "Expected properties section still not to exist");
        }

        [Test]
        public void TestSetExpirationNullOnMessageWithExistingExpiryTime()
        {
            DateTime timestamp = DateTime.UtcNow;

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            amqpNmsMessageFacade.Expiration = timestamp;
            amqpNmsMessageFacade.Expiration = null;

            Assert.AreEqual(default(DateTime), amqpNmsMessageFacade.Message.Properties.AbsoluteExpiryTime, "Expected absolute-expiry-time to be default");
            Assert.IsFalse(amqpNmsMessageFacade.Message.Properties.HasField(8), "Expected absolute-expiry-time is not set");
            Assert.AreEqual(null, amqpNmsMessageFacade.Expiration, "Expected no expiration");
        }

        // --- user-id field  ---

        [Test]
        public void TestGetUserIdIsNullForNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            Assert.IsNull(amqpNmsMessageFacade.UserId);
        }

        [Test]
        public void TestGetUserIdOnReceivedMessage()
        {
            string userIdString = "testValue";
            byte[] bytes = Encoding.UTF8.GetBytes(userIdString);

            global::Amqp.Message message = new global::Amqp.Message
            {
                Properties = new Properties {UserId = bytes}
            };

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            Assert.NotNull(amqpNmsMessageFacade.UserId);
            Assert.AreEqual(userIdString, amqpNmsMessageFacade.UserId);
        }

        [Test]
        public void TestGetUserIdOnReceievedMessageWithEmptyBinaryValue()
        {
            byte[] bytes = new byte[0];
            global::Amqp.Message message = new global::Amqp.Message
            {
                Properties = new Properties {UserId = bytes}
            };

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);
            Assert.Null(amqpNmsMessageFacade.UserId, "Expected a userid on received message");
        }

        /*
         * Check that setting UserId on the message causes creation of the underlying properties
         * section with the expected value.
         */
        [Test]
        public void TestSetUserIdOnNewMessage()
        {
            string userIdString = "testValue";
            byte[] bytes = Encoding.UTF8.GetBytes(userIdString);

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.UserId = userIdString;

            Assert.NotNull(amqpNmsMessageFacade.Properties);
            CollectionAssert.AreEqual(bytes, amqpNmsMessageFacade.Message.Properties.UserId);
            Assert.AreEqual(userIdString, amqpNmsMessageFacade.UserId);
        }

        /*
         * Check that setting UserId null on the message causes any existing value to be cleared
         */
        [Test]
        public void TestSetUserIdNullOnMessageWithExistingUserId()
        {
            string userIdString = "testValue";
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.UserId = userIdString;
            amqpNmsMessageFacade.UserId = null;
            Assert.NotNull(amqpNmsMessageFacade.Properties);
            Assert.Null(amqpNmsMessageFacade.Message.Properties.UserId);
            Assert.Null(amqpNmsMessageFacade.UserId);
        }

        [Test]
        public void TestClearUserIdWithNoExistingProperties()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            amqpNmsMessageFacade.UserId = null;

            Assert.Null(amqpNmsMessageFacade.Message.Properties);
            Assert.Null(amqpNmsMessageFacade.UserId);
        }

        // ====== AMQP Message Annotations =======
        // =======================================

        [Test]
        public void TestNewMessageHasUnderlyingMessageAnnotationsSectionWithTypeAnnotation()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.Null(amqpNmsMessageFacade.MessageAnnotations);
            Assert.AreEqual(MessageSupport.JMS_TYPE_MSG, amqpNmsMessageFacade.JmsMsgType);
        }

        [Test]
        public void TestNewMessageDoesNotHaveUnderlyingMessageAnnotationsSectionWithDeliveryTime()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            Assert.IsNull(amqpNmsMessageFacade.MessageAnnotations);
        }

        [Test]
        public void TestMessageAnnotationExistsUsingReceivedMessageWithoutMessageAnnotationsSection()
        {
            Symbol symbolKeyName = new Symbol("myTestSymbolName");
            global::Amqp.Message message = new global::Amqp.Message();

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            Assert.False(amqpNmsMessageFacade.MessageAnnotationExists(symbolKeyName));
        }

        [Test]
        public void TestMessageAnnotationExistsUsingReceivedMessageWithMessageAnnotationsSection()
        {
            Symbol symbolKeyName = new Symbol("myTestSymbolName");
            string value = "myTestValue";
            global::Amqp.Message message = new global::Amqp.Message
            {
                MessageAnnotations = new MessageAnnotations
                {
                    Map = {{symbolKeyName, value}}
                }
            };

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);
            Assert.True(amqpNmsMessageFacade.MessageAnnotationExists(symbolKeyName));
            Assert.False(amqpNmsMessageFacade.MessageAnnotationExists(new Symbol("otherName")));
        }

        [Test]
        public void TestGetMessageAnnotationUsingReceivedMessageWithoutMessageAnnotationsSection()
        {
            Symbol symbolKeyName = new Symbol("myTestSymbolName");
            global::Amqp.Message message = new global::Amqp.Message();

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);
            Assert.IsNull(amqpNmsMessageFacade.GetMessageAnnotation(symbolKeyName));
        }

        [Test]
        public void TestGetMessageAnnotationUsingReceivedMessage()
        {
            Symbol symbolKeyName = new Symbol("myTestSymbolName");
            string value = "myTestValue";
            global::Amqp.Message message = new global::Amqp.Message
            {
                MessageAnnotations = new MessageAnnotations
                {
                    Map = {{symbolKeyName, value}}
                }
            };

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            Assert.AreEqual(value, amqpNmsMessageFacade.GetMessageAnnotation(symbolKeyName));
            Assert.Null(amqpNmsMessageFacade.GetMessageAnnotation(new Symbol("otherName")));
        }

        [Test]
        public void TestSetMessageAnnotationsOnNewMessage()
        {
            Symbol symbolKeyName = new Symbol("myTestSymbolName");
            Symbol symbolKeyName2 = new Symbol("myTestSymbolName2");
            string value = "myTestValue";

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            // check setting first annotation
            amqpNmsMessageFacade.SetMessageAnnotation(symbolKeyName, value);

            MessageAnnotations underlyingAnnotations = amqpNmsMessageFacade.MessageAnnotations;
            Assert.NotNull(underlyingAnnotations);

            Assert.True(underlyingAnnotations.Map.ContainsKey(symbolKeyName));
            Assert.AreEqual(value, underlyingAnnotations.Map[symbolKeyName]);

            // set another
            amqpNmsMessageFacade.SetMessageAnnotation(symbolKeyName2, value);

            Assert.True(underlyingAnnotations.Map.ContainsKey(symbolKeyName));
            Assert.True(underlyingAnnotations.Map.ContainsKey(symbolKeyName2));
        }

        [Test]
        public void TestGetJmsTypeIsNullOnNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            Assert.Null(amqpNmsMessageFacade.NMSType, "did not expect a JMSType value to be present");
        }

        [Test]
        public void TestSetNmsTypeSetsUnderlyingMessageSubject()
        {
            string nmsType = "myJMSType";
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            amqpNmsMessageFacade.NMSType = nmsType;

            Assert.AreEqual(nmsType, amqpNmsMessageFacade.Message.Properties.Subject);
        }

        [Test]
        public void TestSetTypeNullClearsExistingSubjectValue()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();

            Assert.Null(amqpNmsMessageFacade.Message.Properties, "Should not be any Properties object by default");
            amqpNmsMessageFacade.NMSType = null;


            Assert.Null(amqpNmsMessageFacade.Message.Properties, "Subject should be clear");
            Assert.Null(amqpNmsMessageFacade.NMSType);
        }

        [Test]
        public void TestGetNmsTypeWithReceivedMessage()
        {
            String myNMSType = "myJMSType";

            global::Amqp.Message message = new global::Amqp.Message
            {
                Properties = new Properties
                {
                    Subject = myNMSType,
                }
            };

            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateReceivedMessageFacade(message);

            Assert.AreEqual(myNMSType, amqpNmsMessageFacade.NMSType);
        }
        
        // ====== Content Type =======
        
        [Test]
        public void TestGetContentTypeIsNullOnNewMessage()
        {
            AmqpNmsMessageFacade amqpNmsMessageFacade = CreateNewMessageFacade();
            Assert.IsNull(amqpNmsMessageFacade.ContentType);
        }

        [Test]
        public void TestCopyOfEmptyMessageSucceeds()
        {
            AmqpNmsMessageFacade empty = CreateNewMessageFacade();
            INmsMessageFacade copy = empty.Copy();
            Assert.NotNull(copy);
        }

        [Test]
        public void TestBasicMessageCopy()
        {
            var source = CreateNewMessageFacade();
            source.SetMessageAnnotation("test-annotation", "value");

            var queue = new NmsQueue("Test-Queue");
            var temporaryQueue = new NmsTemporaryQueue("Test-Temp-Queue");
            source.NMSDestination = queue;
            source.NMSReplyTo = temporaryQueue;
            source.ContentType = "Test-ContentType";
            source.NMSCorrelationID = "MY-APP-ID";
            source.Expiration = DateTime.UtcNow;
            source.GroupId = "TEST-GROUP";
            source.GroupSequence = 23;
            source.NMSMessageId = "ID:TEST-MESSAGEID";
            source.NMSPriority = MsgPriority.High;
            source.RedeliveryCount = 12;
            source.IsPersistent = true;
            source.NMSTimestamp = DateTime.UtcNow;
            source.NMSTimeToLive = TimeSpan.FromDays(1);
            source.UserId = "Cookie-Monster";
            source.Properties.SetString("APP-Prop-1", "APP-Prop-1-Value");
            source.Properties.SetString("APP-Prop-2", "APP-Prop-2-Value");

            var copy = source.Copy() as AmqpNmsMessageFacade;
            
            Assert.IsNotNull(copy);
            Assert.AreEqual(source.NMSDestination, copy.NMSDestination);
            Assert.AreEqual(source.NMSReplyTo, copy.NMSReplyTo);
            Assert.AreEqual(source.ContentType, copy.ContentType);
            Assert.AreEqual(source.NMSCorrelationID, copy.NMSCorrelationID);
            Assert.AreEqual(source.GroupId, copy.GroupId);
            Assert.AreEqual(source.GroupSequence, copy.GroupSequence);
            Assert.AreEqual(source.NMSMessageId, copy.NMSMessageId);
            Assert.AreEqual(source.NMSPriority, copy.NMSPriority);
            Assert.AreEqual(source.RedeliveryCount, copy.RedeliveryCount);
            Assert.AreEqual(source.IsPersistent, copy.IsPersistent);
            Assert.AreEqual(source.UserId, copy.UserId);
            Assert.AreEqual(source.NMSTimeToLive, copy.NMSTimeToLive);
            Assert.IsTrue(Math.Abs((copy.Expiration.Value - source.Expiration.Value).TotalMilliseconds) < 1);
            Assert.IsTrue(Math.Abs((copy.NMSTimestamp - source.NMSTimestamp).TotalMilliseconds) < 1);
            Assert.AreEqual(source.Properties.GetString("APP-Prop-1"), copy.Properties.GetString("APP-Prop-1"));
            Assert.AreEqual(source.GetMessageAnnotation("test-annotation"), copy.GetMessageAnnotation("test-annotation"));
        }
    }
}