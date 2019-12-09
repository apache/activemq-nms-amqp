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
using System.Collections;
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS;
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Apache.NMS.AMQP.Util;
using Moq;
using NUnit.Framework;
using List = Amqp.Types.List;

namespace NMS.AMQP.Test.Provider.Amqp
{
    [TestFixture]
    public class AmqpCodecTest : AmqpNmsMessageTypesTestCase
    {
        [Test]
        public void TestPersistentSetFromMessageWithNonDefaultValue()
        {
            var message = new global::Amqp.Message
            {
                Header = new Header { Durable = true },
                BodySection = new AmqpValue { Value = "test" }
            };

            NmsMessage nmsMessage = AmqpCodec.DecodeMessage(CreateMockConsumer(), message).AsMessage();
            Assert.NotNull(nmsMessage);
            Assert.IsInstanceOf<NmsMessage>(nmsMessage);
            Assert.AreEqual(MsgDeliveryMode.Persistent, nmsMessage.NMSDeliveryMode);

            INmsMessageFacade facade = nmsMessage.Facade;
            Assert.NotNull(facade);
            Assert.IsInstanceOf<AmqpNmsMessageFacade>(facade);
        }

        [Test]
        public void TestMessagePrioritySetFromMessageWithNonDefaultValue()
        {
            var message = new global::Amqp.Message
            {
                Header = new Header { Priority = 8 },
                BodySection = new AmqpValue { Value = "test" }
            };

            NmsMessage nmsMessage = AmqpCodec.DecodeMessage(CreateMockConsumer(), message).AsMessage();

            Assert.NotNull(nmsMessage);
            Assert.IsInstanceOf<NmsMessage>(nmsMessage);
            Assert.AreEqual(MsgPriority.VeryHigh, nmsMessage.NMSPriority);

            AmqpNmsMessageFacade facade = nmsMessage.Facade as AmqpNmsMessageFacade;
            Assert.NotNull(facade);
            Assert.AreEqual(MsgPriority.VeryHigh, facade.NMSPriority);
        }

        [Test]
        public void TestTimeToLiveSetFromMessageWithNonDefaultValue()
        {
            var message = new global::Amqp.Message
            {
                Header = new Header { Ttl = 65535 },
                BodySection = new AmqpValue { Value = "test" }
            };

            NmsMessage nmsMessage = AmqpCodec.DecodeMessage(CreateMockConsumer(), message).AsMessage();

            Assert.NotNull(nmsMessage);
            Assert.IsInstanceOf<NmsMessage>(nmsMessage);
            Assert.AreEqual(TimeSpan.FromMilliseconds(65535), nmsMessage.NMSTimeToLive);

            AmqpNmsMessageFacade facade = nmsMessage.Facade as AmqpNmsMessageFacade;
            Assert.NotNull(facade);
            Assert.AreEqual(TimeSpan.FromMilliseconds(65535), facade.NMSTimeToLive);
        }

        [Test]
        public void TestDeliveryCountSetFromMessageWithNonDefaultValue()
        {
            var message = new global::Amqp.Message
            {
                Header = new Header { DeliveryCount = 2 },
                BodySection = new AmqpValue { Value = "test" }
            };

            NmsMessage nmsMessage = AmqpCodec.DecodeMessage(CreateMockConsumer(), message).AsMessage();

            Assert.NotNull(nmsMessage);
            Assert.IsInstanceOf<NmsMessage>(nmsMessage);
            Assert.IsTrue(nmsMessage.NMSRedelivered);

            AmqpNmsMessageFacade facade = nmsMessage.Facade as AmqpNmsMessageFacade;
            Assert.NotNull(facade);
            Assert.AreEqual(2, facade.RedeliveryCount);
            Assert.AreEqual(2, facade.Message.Header.DeliveryCount);
        }

        // =============== With The Message Type Annotation =========
        // ==========================================================

        [Test]
        public void TestCreateMessageFromUnknownMessageTypeAnnotationValueThrows()
        {
            var message = new global::Amqp.Message
            {
                MessageAnnotations = new MessageAnnotations
                {
                    Map = { { SymbolUtil.JMSX_OPT_MSG_TYPE, -1 } }
                }
            };

            Assert.Catch<Exception>(() => AmqpCodec.DecodeMessage(CreateMockConsumer(), message));
        }

        [Test]
        public void TestCreateGenericMessageFromMessageTypeAnnotation()
        {
            var message = CreateMessageWithTypeAnnotation(MessageSupport.JMS_TYPE_MSG);

            NmsMessage nmsMessage = AmqpCodec.DecodeMessage(CreateMockConsumer(), message).AsMessage();
            Assert.NotNull(nmsMessage);

            INmsMessageFacade facade = nmsMessage.Facade;
            Assert.NotNull(facade);
            Assert.IsInstanceOf<AmqpNmsMessageFacade>(facade);
        }

        [Test]
        public void TestCreateBytesMessageFromMessageTypeAnnotation()
        {
            var message = CreateMessageWithTypeAnnotation(MessageSupport.JMS_TYPE_BYTE);

            NmsMessage nmsMessage = AmqpCodec.DecodeMessage(CreateMockConsumer(), message).AsMessage();
            Assert.NotNull(nmsMessage);
            Assert.IsInstanceOf<NmsBytesMessage>(nmsMessage);

            INmsMessageFacade facade = nmsMessage.Facade;
            Assert.NotNull(facade);
            Assert.IsInstanceOf<AmqpNmsBytesMessageFacade>(facade);
        }

        [Test]
        public void TestCreateTextMessageFromMessageTypeAnnotation()
        {
            var message = CreateMessageWithTypeAnnotation(MessageSupport.JMS_TYPE_TXT);

            AssertDecodeTextMessage(message);
        }

        [Test]
        public void TestCreateObjectMessageFromMessageTypeAnnotation()
        {
            CreateObjectMessageFromMessageTypeAnnotationTestImpl(true);
        }

        [Test]
        public void TestCreateObjectMessageFromMessageTypeAnnotationAnd()
        {
            CreateObjectMessageFromMessageTypeAnnotationTestImpl(false);
        }

        private void CreateObjectMessageFromMessageTypeAnnotationTestImpl(bool setDotnetSerializedContentType)
        {
            global::Amqp.Message message = CreateMessageWithTypeAnnotation(MessageSupport.JMS_TYPE_OBJ);
            if (setDotnetSerializedContentType)
            {
                message.Properties = new Properties
                {
                    ContentType = MessageSupport.SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE
                };
            }

            AssertDecodeObjectMessage(message, setDotnetSerializedContentType);
        }

        [Test]
        public void TestCreateStreamMessageFromMessageTypeAnnotation()
        {
            var message = CreateMessageWithTypeAnnotation(MessageSupport.JMS_TYPE_STRM);

            NmsMessage nmsMessage = AmqpCodec.DecodeMessage(CreateMockConsumer(), message).AsMessage();
            Assert.NotNull(nmsMessage);
            Assert.IsInstanceOf<NmsStreamMessage>(nmsMessage);

            INmsMessageFacade facade = nmsMessage.Facade;
            Assert.NotNull(facade);
            Assert.IsInstanceOf<AmqpNmsStreamMessageFacade>(facade);
        }

        // =============== Without The Message Type Annotation =========
        // =============================================================

        // --------- No Body Section ---------

        [Test]
        public void TestCreateBytesMessageFromNoBodySectionAndContentType()
        {
            global::Amqp.Message message = new global::Amqp.Message
            {
                Properties = new Properties
                {
                    ContentType = SymbolUtil.OCTET_STREAM_CONTENT_TYPE
                }
            };

            NmsMessage nmsMessage = AmqpCodec.DecodeMessage(CreateMockConsumer(), message).AsMessage();
            Assert.NotNull(nmsMessage);
            Assert.IsInstanceOf<NmsBytesMessage>(nmsMessage);

            INmsMessageFacade facade = nmsMessage.Facade;
            Assert.NotNull(facade);
            Assert.IsInstanceOf<AmqpNmsBytesMessageFacade>(facade);
        }

        [Test]
        public void TestCreateBytesMessageFromNoBodySectionAndNoContentType()
        {
            var message = new global::Amqp.Message();
            Assert.IsNull(message.BodySection);

            NmsMessage nmsMessage = AmqpCodec.DecodeMessage(CreateMockConsumer(), message).AsMessage();
            Assert.NotNull(nmsMessage);
            Assert.IsInstanceOf<NmsMessage>(nmsMessage);

            INmsMessageFacade facade = nmsMessage.Facade;
            Assert.NotNull(facade);
            Assert.IsInstanceOf<AmqpNmsMessageFacade>(facade);
        }

        [Test]
        public void TestCreateObjectMessageFromNoBodySectionAndContentType()
        {
            var message = new global::Amqp.Message
            {
                Properties = new Properties
                {
                    ContentType = MessageSupport.SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE
                }
            };

            AssertDecodeObjectMessage(message, true);
        }

        [Test]
        public void TestCreateTextMessageFromNoBodySectionAndContentType()
        {
            var message = new global::Amqp.Message
            {
                Properties = new Properties
                {
                    ContentType = new Symbol("text/plain")
                }
            };

            AssertDecodeTextMessage(message);
        }

        [Test]
        public void TestCreateGenericMessageFromNoBodySectionAndUnknownContentType()
        {
            var message = new global::Amqp.Message
            {
                Properties = new Properties
                {
                    ContentType = new Symbol("unknown-content-type")
                }
            };

            NmsMessage nmsMessage = AmqpCodec.DecodeMessage(CreateMockConsumer(), message).AsMessage();
            Assert.NotNull(nmsMessage);
            Assert.IsInstanceOf<NmsMessage>(nmsMessage);

            INmsMessageFacade facade = nmsMessage.Facade;
            Assert.NotNull(facade);
            Assert.IsInstanceOf<AmqpNmsMessageFacade>(facade);
        }

        // --------- Data Body Section ---------

        [Test]
        public void TestCreateBytesMessageFromDataWithEmptyBinaryAndContentType()
        {
            var message = new global::Amqp.Message
            {
                BodySection = new Data
                {
                    Binary = new byte[0]
                },
                Properties = new Properties
                {
                    ContentType = SymbolUtil.OCTET_STREAM_CONTENT_TYPE.ToString()
                }
            };

            AssertDecodeBytesMessage(message);
        }

        [Test]
        public void TestCreateBytesMessageFromDataWithUnknownContentType()
        {
            var message = new global::Amqp.Message
            {
                BodySection = new Data
                {
                    Binary = new byte[0]
                },
                Properties = new Properties
                {
                    ContentType = "unknown-content-type"
                }
            };

            NmsMessage nmsMessage = AmqpCodec.DecodeMessage(CreateMockConsumer(), message).AsMessage();
            Assert.NotNull(nmsMessage);
            Assert.IsInstanceOf<NmsBytesMessage>(nmsMessage);

            INmsMessageFacade facade = nmsMessage.Facade;
            Assert.NotNull(facade);
            Assert.IsInstanceOf<AmqpNmsBytesMessageFacade>(facade);
        }

        [Test]
        public void TestCreateBytesMessageFromDataWithEmptyBinaryAndNoContentType()
        {
            var message = new global::Amqp.Message
            {
                BodySection = new Data { Binary = new byte[0] },
            };

            AssertDecodeBytesMessage(message);
        }

        [Test]
        public void TestCreateObjectMessageFromDataWithContentTypeAndEmptyBinary()
        {
            var message = new global::Amqp.Message
            {
                BodySection = new Data
                {
                    Binary = new byte[0]
                },
                Properties = new Properties
                {
                    ContentType = MessageSupport.SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE
                }
            };

            AssertDecodeObjectMessage(message, true);
        }

        // --------- AmqpValue Body Section ---------

        [Test]
        public void TestCreateTextMessageFromAmqpValueWithString()
        {
            var message = new global::Amqp.Message
            {
                BodySection = new AmqpValue { Value = "content" },
            };

            AssertDecodeTextMessage(message);
        }

        [Test]
        public void TestCreateTextMessageFromAmqpValueWithNull()
        {
            var message = new global::Amqp.Message
            {
                BodySection = new AmqpValue { Value = null },
            };

            AssertDecodeTextMessage(message);
        }

        [Test]
        public void TestCreateAmqpObjectMessageFromAmqpValueWithMap()
        {
            var message = new global::Amqp.Message
            {
                BodySection = new AmqpValue { Value = new Map() },
            };

            AssertDecodeObjectMessage(message, false);
        }

        [Test]
        public void TestCreateAmqpObjectMessageFromAmqpValueWithList()
        {
            var message = new global::Amqp.Message
            {
                BodySection = new AmqpValue { Value = new List() },
            };

            AssertDecodeObjectMessage(message, false);
        }

        [Test]
        public void TestCreateAmqpBytesMessageFromAmqpValueWithBinary()
        {
            var message = new global::Amqp.Message
            {
                BodySection = new AmqpValue { Value = new byte[0] },
            };

            AssertDecodeBytesMessage(message);
        }

        [Test]
        public void TestCreateObjectMessageFromAmqpValueWithUncategorisedContent()
        {
            var message = new global::Amqp.Message
            {
                BodySection = new AmqpValue { Value = Guid.NewGuid() },
            };

            AssertDecodeObjectMessage(message, false);
        }

        // --------- AmqpSequence Body Section ---------

        [Test]
        public void TestCreateObjectMessageMessageFromAmqpSequence()
        {
            var message = new global::Amqp.Message
            {
                BodySection = new AmqpSequence { List = new ArrayList() },
            };

            AssertDecodeObjectMessage(message, false);
        }

        //----- Message Annotation Handling --------------------------------------//

        [Test]
        public void TestNMSMessageEncodingAddsProperMessageAnnotations()
        {
            DoTestNMSMessageEncodingAddsProperMessageAnnotations(MessageSupport.JMS_TYPE_MSG, MessageSupport.JMS_DEST_TYPE_QUEUE, null);
            DoTestNMSMessageEncodingAddsProperMessageAnnotations(MessageSupport.JMS_TYPE_BYTE, MessageSupport.JMS_DEST_TYPE_QUEUE, MessageSupport.JMS_DEST_TYPE_TEMP_QUEUE);
            DoTestNMSMessageEncodingAddsProperMessageAnnotations(MessageSupport.JMS_TYPE_MAP, null, MessageSupport.JMS_DEST_TYPE_TEMP_QUEUE);
            DoTestNMSMessageEncodingAddsProperMessageAnnotations(MessageSupport.JMS_TYPE_TXT, MessageSupport.JMS_DEST_TYPE_TOPIC, MessageSupport.JMS_DEST_TYPE_TEMP_QUEUE);
            DoTestNMSMessageEncodingAddsProperMessageAnnotations(MessageSupport.JMS_TYPE_STRM, MessageSupport.JMS_DEST_TYPE_QUEUE, MessageSupport.JMS_DEST_TYPE_TEMP_QUEUE);
        }

        private void DoTestNMSMessageEncodingAddsProperMessageAnnotations(sbyte msgType, byte? toType, byte? replyToType)
        {
            AmqpNmsMessageFacade messageFacade = CreateMessageFacadeFromTypeId(msgType);
            IDestination to = CreateDestinationFromTypeId(toType);
            IDestination replyTo = CreateDestinationFromTypeId(replyToType);

            messageFacade.NMSDestination = to;
            messageFacade.NMSReplyTo = replyTo;

            AmqpCodec.EncodeMessage(messageFacade);
            Assert.AreEqual(messageFacade.MessageAnnotations[SymbolUtil.JMSX_OPT_MSG_TYPE], msgType);

            if (toType != null)
            {
                Assert.True(messageFacade.MessageAnnotationExists(SymbolUtil.JMSX_OPT_DEST));
                Assert.AreEqual(messageFacade.GetMessageAnnotation(SymbolUtil.JMSX_OPT_DEST), toType.Value);
            }

            if (replyToType != null)
            {
                Assert.True(messageFacade.MessageAnnotationExists(SymbolUtil.JMSX_OPT_REPLY_TO));
                Assert.AreEqual(messageFacade.GetMessageAnnotation(SymbolUtil.JMSX_OPT_REPLY_TO), replyToType.Value);
            }
        }

        private void AssertDecodeTextMessage(global::Amqp.Message message)
        {
            NmsMessage nmsMessage = AmqpCodec.DecodeMessage(CreateMockConsumer(), message).AsMessage();
            Assert.NotNull(nmsMessage);
            Assert.IsInstanceOf<NmsTextMessage>(nmsMessage);

            INmsMessageFacade facade = nmsMessage.Facade;
            Assert.NotNull(facade);
            Assert.IsInstanceOf<AmqpNmsTextMessageFacade>(facade);
        }

        private void AssertDecodeBytesMessage(global::Amqp.Message message)
        {
            NmsMessage nmsMessage = AmqpCodec.DecodeMessage(CreateMockConsumer(), message).AsMessage();
            Assert.NotNull(nmsMessage);
            Assert.IsInstanceOf<NmsBytesMessage>(nmsMessage);

            INmsMessageFacade facade = nmsMessage.Facade;
            Assert.NotNull(facade);
            Assert.IsInstanceOf<AmqpNmsBytesMessageFacade>(facade);
        }

        private void AssertDecodeObjectMessage(global::Amqp.Message message, bool setDotnetSerializedContentType)
        {
            NmsMessage nmsMessage = AmqpCodec.DecodeMessage(CreateMockConsumer(), message).AsMessage();
            Assert.NotNull(nmsMessage);
            Assert.IsInstanceOf<NmsObjectMessage>(nmsMessage);

            AmqpNmsObjectMessageFacade facade = nmsMessage.Facade as AmqpNmsObjectMessageFacade;
            Assert.NotNull(facade);
            if (setDotnetSerializedContentType)
                Assert.IsInstanceOf<AmqpSerializedObjectDelegate>(facade.Delegate);
            else
                Assert.IsInstanceOf<AmqpTypedObjectDelegate>(facade.Delegate);
        }

        private static global::Amqp.Message CreateMessageWithTypeAnnotation(sbyte typeAnnotation)
        {
            return new global::Amqp.Message
            {
                MessageAnnotations = new MessageAnnotations
                {
                    Map = { { SymbolUtil.JMSX_OPT_MSG_TYPE, typeAnnotation } }
                }
            };
        }

        private AmqpNmsMessageFacade CreateMessageFacadeFromTypeId(sbyte msgType)
        {
            AmqpNmsMessageFacade message;
            switch (msgType)
            {
                case MessageSupport.JMS_TYPE_MSG:
                    message = new AmqpNmsMessageFacade();
                    break;
                case MessageSupport.JMS_TYPE_BYTE:
                    message = new AmqpNmsBytesMessageFacade();
                    break;
                case MessageSupport.JMS_TYPE_TXT:
                    message = new AmqpNmsTextMessageFacade();
                    break;
                case MessageSupport.JMS_TYPE_OBJ:
                    message = new AmqpNmsObjectMessageFacade();
                    break;
                case MessageSupport.JMS_TYPE_STRM:
                    message = new AmqpNmsStreamMessageFacade();
                    break;
                case MessageSupport.JMS_TYPE_MAP:
                    message = new AmqpNmsMapMessageFacade();
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(msgType));
            }

            message.Initialize(CreateMockAmqpConnection());

            return message;
        }

        private IDestination CreateDestinationFromTypeId(byte? destinationType)
        {
            switch (destinationType)
            {
                case MessageSupport.JMS_DEST_TYPE_QUEUE:
                    return new NmsQueue("test");
                case MessageSupport.JMS_DEST_TYPE_TOPIC:
                    return new NmsTopic("test");
                case MessageSupport.JMS_DEST_TYPE_TEMP_QUEUE:
                    return new NmsTemporaryQueue("test");
                case MessageSupport.JMS_DEST_TYPE_TEMP_TOPIC:
                    return new NmsTemporaryTopic("test");
                default:
                    return null;
            }
        }
    }
}