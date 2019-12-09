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
using Apache.NMS.AMQP.Provider.Amqp;
using Apache.NMS.AMQP.Provider.Amqp.Message;

namespace Apache.NMS.AMQP.Util
{
    public static class AmqpDestinationHelper
    {
        public static string GetDestinationAddress(IDestination destination, IAmqpConnection connection)
        {
            if (destination != null)
            {
                string qPrefix = null;
                string tPrefix = null;
                if (!destination.IsTemporary)
                {
                    qPrefix = connection.QueuePrefix;
                    tPrefix = connection.TopicPrefix;
                }

                string destinationName = null;
                string prefix = null;
                if (destination.IsQueue)
                {
                    destinationName = (destination as IQueue)?.QueueName;
                    prefix = qPrefix ?? string.Empty;
                }
                else
                {
                    destinationName = (destination as ITopic)?.TopicName;
                    prefix = tPrefix ?? string.Empty;
                }

                if (destinationName != null)
                {
                    if (!destinationName.StartsWith(prefix))
                    {
                        destinationName = prefix + destinationName;
                    }
                }

                return destinationName;
            }
            else
            {
                return null;
            }
        }

        public static IDestination GetDestination(AmqpNmsMessageFacade message, IAmqpConnection connection, IDestination consumerDestination)
        {
            string to = message.ToAddress;

            object typeAnnotation = message.GetMessageAnnotation(SymbolUtil.JMSX_OPT_DEST);
            if (typeAnnotation != null)
            {
                byte type = Convert.ToByte(typeAnnotation);
                string name = StripPrefixIfNecessary(to, connection, type);
                return CreateDestination(name, type);
            }
            else
            {
                string name = StripPrefixIfNecessary(to, connection);
                return CreateDestination(name, consumerDestination, false);
            }
        }

        public static IDestination GetReplyTo(AmqpNmsMessageFacade message, IAmqpConnection connection, IDestination consumerDestination)
        {
            string replyTo = message.ReplyToAddress;

            object typeAnnotation = message.GetMessageAnnotation(SymbolUtil.JMSX_OPT_REPLY_TO);
            if (typeAnnotation != null)
            {
                byte type = Convert.ToByte(typeAnnotation);
                string name = StripPrefixIfNecessary(replyTo, connection, type);
                return CreateDestination(name, type);
            }
            else
            {
                string name = StripPrefixIfNecessary(replyTo, connection);
                return CreateDestination(name, consumerDestination, true);
            }
        }

        private static string StripPrefixIfNecessary(string address, IAmqpConnection connection, byte type)
        {
            if (address == null)
                return null;

            if (type == MessageSupport.JMS_DEST_TYPE_QUEUE)
            {
                string queuePrefix = connection.QueuePrefix;
                if (queuePrefix != null && address.StartsWith(queuePrefix))
                {
                    return address.Substring(queuePrefix.Length);
                }
            }
            else if (type == MessageSupport.JMS_DEST_TYPE_TOPIC)
            {
                string topicPrefix = connection.TopicPrefix;
                if (topicPrefix != null && address.StartsWith(topicPrefix))
                {
                    return address.Substring(topicPrefix.Length);
                }
            }

            return address;
        }

        private static string StripPrefixIfNecessary(string address, IAmqpConnection connection)
        {
            if (address == null)
                return null;

            string queuePrefix = connection.QueuePrefix;
            if (queuePrefix != null && address.StartsWith(queuePrefix))
            {
                return address.Substring(queuePrefix.Length);
            }

            string topicPrefix = connection.TopicPrefix;
            if (topicPrefix != null && address.StartsWith(topicPrefix))
            {
                return address.Substring(topicPrefix.Length);
            }

            return address;
        }

        private static IDestination CreateDestination(string address, byte typeByte)
        {
            if (address == null)
                return null;

            switch (typeByte)
            {
                case MessageSupport.JMS_DEST_TYPE_QUEUE:
                    return new NmsQueue(address);
                case MessageSupport.JMS_DEST_TYPE_TOPIC:
                    return new NmsTopic(address);
                case MessageSupport.JMS_DEST_TYPE_TEMP_QUEUE:
                    NmsTemporaryQueue temporaryQueue = new NmsTemporaryQueue(address);
                    temporaryQueue.Address = address;
                    return temporaryQueue;
                case MessageSupport.JMS_DEST_TYPE_TEMP_TOPIC:
                    NmsTemporaryTopic temporaryTopic = new NmsTemporaryTopic(address);
                    temporaryTopic.Address = address;
                    return temporaryTopic;
            }

            // fall back to a Queue Destination since we need a real NMS destination
            return new NmsQueue(address);
        }

        private static IDestination CreateDestination(string address, IDestination consumerDestination, bool useConsumerDestForTypeOnly)
        {
            if (address == null)
            {
                return useConsumerDestForTypeOnly ? null : consumerDestination;
            }

            if (consumerDestination.IsQueue)
            {
                if (consumerDestination.IsTemporary)
                    return new NmsTemporaryQueue(address);
                else
                    return new NmsQueue(address);
            }
            else if (consumerDestination.IsTopic)
            {
                if (consumerDestination.IsTemporary)
                    return new NmsTemporaryTopic(address);
                else
                    return new NmsTopic(address);
            }

            // fall back to a Queue Destination since we need a real NMS destination
            return new NmsQueue(address);
        }

        public static void SetToAnnotationFromDestination(IDestination destination, MessageAnnotations annotations)
            => SetAnnotationFromDestination(SymbolUtil.JMSX_OPT_DEST, destination, annotations);

        public static void SetReplyToAnnotationFromDestination(IDestination destination, MessageAnnotations annotations)
            => SetAnnotationFromDestination(SymbolUtil.JMSX_OPT_REPLY_TO, destination, annotations);

        private static void SetAnnotationFromDestination(Symbol key, IDestination destination, MessageAnnotations annotations)
        {
            byte? typeValue = ToTypeAnnotation(destination);

            if (typeValue == null)
                annotations.Map.Remove(key);
            else
                annotations.Map[key] = typeValue;
        }

        private static byte? ToTypeAnnotation(IDestination destination)
        {
            if (destination == null)
                return null;

            if (destination.IsQueue)
            {
                if (destination.IsTemporary)
                    return MessageSupport.JMS_DEST_TYPE_TEMP_QUEUE;
                else
                    return MessageSupport.JMS_DEST_TYPE_QUEUE;
            }
            else if (destination.IsTopic)
            {
                if (destination.IsTemporary)
                    return MessageSupport.JMS_DEST_TYPE_TEMP_TOPIC;
                else
                    return MessageSupport.JMS_DEST_TYPE_TOPIC;
            }

            return null;
        }
    }
}