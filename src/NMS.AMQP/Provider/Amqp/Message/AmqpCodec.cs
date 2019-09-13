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
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp.Message
{
    public class AmqpCodec
    {
        public static INmsMessageFacade DecodeMessage(IAmqpConsumer consumer, global::Amqp.Message amqpMessage)
        {
            // First we try the easy way, if the annotation is there we don't have to work hard.
            AmqpNmsMessageFacade result = CreateFromMsgAnnotation(amqpMessage);
            if (result == null)
            {
                // Next, match specific section structures and content types
                result = CreateWithoutAnnotation(amqpMessage.BodySection, amqpMessage.Properties);
            }

            if (result != null)
            {
                result.Initialize(consumer, amqpMessage);
                return result;
            }

            throw new NMSException("Could not create a NMS message from incoming message");
        }

        private static AmqpNmsMessageFacade CreateFromMsgAnnotation(global::Amqp.Message message)
        {
            object annotation = message.MessageAnnotations?[SymbolUtil.JMSX_OPT_MSG_TYPE];

            if (annotation != null)
            {
                sbyte type = Convert.ToSByte(annotation);
                switch (type)
                {
                    case MessageSupport.JMS_TYPE_MSG:
                        return new AmqpNmsMessageFacade();
                    case MessageSupport.JMS_TYPE_TXT:
                        return new AmqpNmsTextMessageFacade();
                    case MessageSupport.JMS_TYPE_STRM:
                        return new AmqpNmsStreamMessageFacade();
                    case MessageSupport.JMS_TYPE_MAP:
                        return new AmqpNmsMapMessageFacade();
                    case MessageSupport.JMS_TYPE_BYTE:
                    // Java serialized objects should be treated as bytes messages
                    // as we cannot deserialize them in .NET world
                    case MessageSupport.JMS_TYPE_OBJ when IsContentType(SymbolUtil.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE, GetContentType(message.Properties)):
                        return new AmqpNmsBytesMessageFacade();
                    case MessageSupport.JMS_TYPE_OBJ:
                        return new AmqpNmsObjectMessageFacade();
                    default:
                        throw new NMSException("Invalid Message Type annotation value found in message: " + annotation);
                }
            }

            return null;
        }

        private static AmqpNmsMessageFacade CreateWithoutAnnotation(RestrictedDescribed body, Properties properties)
        {
            Symbol contentType = GetContentType(properties);

            if (body is Data || body is null)
            {
                if (IsContentType(SymbolUtil.OCTET_STREAM_CONTENT_TYPE, contentType) || IsContentType(null, contentType))
                    return new AmqpNmsBytesMessageFacade();
                else if (IsContentType(SymbolUtil.SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE, contentType))
                    return new AmqpNmsObjectMessageFacade();
                else
                {
                    if (IsTextualContent(contentType))
                        return new AmqpNmsTextMessageFacade();
                    else
                        return new AmqpNmsBytesMessageFacade();
                }
            }
            else if (body is AmqpValue amqpValue)
            {
                object value = amqpValue.Value;
                if (value == null || value is string)
                    return new AmqpNmsTextMessageFacade();
                else if (value is byte[])
                    return new AmqpNmsBytesMessageFacade();
                else
                    return new AmqpNmsObjectMessageFacade();
            }
            else if (body is AmqpSequence)
                return new AmqpNmsObjectMessageFacade();

            return null;
        }

        private static bool IsTextualContent(Symbol contentType)
        {
            return contentType != null && contentType.ToString().Equals("text/plain", StringComparison.InvariantCultureIgnoreCase);
        }

        private static bool IsContentType(Symbol contentType, Symbol messageContentType)
        {
            if (contentType == null)
                return messageContentType == null;
            if (messageContentType == null)
                return false;
            return contentType.Equals(messageContentType);
        }

        private static Symbol GetContentType(Properties properties)
        {
            return properties?.ContentType;
        }

        public static void EncodeMessage(AmqpNmsMessageFacade messageFacade)
        {
            if (messageFacade.Message.MessageAnnotations == null) 
                messageFacade.Message.MessageAnnotations = new MessageAnnotations();

            if (messageFacade.JmsMsgType.HasValue) 
                messageFacade.Message.MessageAnnotations[SymbolUtil.JMSX_OPT_MSG_TYPE] = messageFacade.JmsMsgType.Value;

            AmqpDestinationHelper.SetToAnnotationFromDestination(messageFacade.NMSDestination, messageFacade.Message.MessageAnnotations);
            AmqpDestinationHelper.SetReplyToAnnotationFromDestination(messageFacade.NMSReplyTo, messageFacade.Message.MessageAnnotations);
        }
    }
}