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

using System.Collections;
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.AMQP.Util;
using Apache.NMS.Util;

namespace Apache.NMS.AMQP.Provider.Amqp.Message
{
    public class AmqpNmsStreamMessageFacade : AmqpNmsMessageFacade, INmsStreamMessageFacade
    {
        private IList list;
        private int position = 0;
        
        public override sbyte? JmsMsgType => MessageSupport.JMS_TYPE_STRM;

        public object Peek()
        {
            if (IsEmpty || position >= list.Count)
            {
                throw new MessageEOFException("Attempt to read past end of stream");
            }

            object value = list[position];
            if (value is byte[] binary)
            {
                // Copy to a byte[], ensure we copy only the required portion.
                byte[] bin = new byte[binary.Length];
                binary.CopyTo(bin, 0);
                value = bin;
            }

            return value;
        }

        private bool IsEmpty => list.Count <= 0;

        public void Pop()
        {
            if (IsEmpty || position >= list.Count)
            {
                throw new MessageEOFException("Attempt to read past end of stream");
            }

            position++;
        }

        public void Reset()
        {
            position = 0;
        }

        public void Put(object value)
        {
            object entry = value;
            if (entry != null && entry is byte[] bytes)
            {
                byte[] bin = new byte[bytes.Length];
                bytes.CopyTo(bin, 0);
                entry = bin;
            }
            if (list.Add(entry) < 0)
            {
                throw NMSExceptionSupport.Create($"Failed to add {entry.ToString()} to stream.", null);
            }
        }

        public sbyte JmsMessageType => MessageSupport.JMS_TYPE_STRM;

        protected override void InitializeBody()
        {
            if (Message.BodySection == null)
            {
                list = InitializeEmptyBody(true);
            }
            else if (Message.BodySection is AmqpSequence amqpSequence)
            {
                list = amqpSequence.List ?? InitializeEmptyBody(true);
            }
            else if (Message.BodySection is AmqpValue amqpValue)
            {
                object value = amqpValue.Value;
                if (value == null)
                {
                    list = InitializeEmptyBody(false);
                }
                else if (value is IList valueList)
                {
                    list = valueList;
                }
                else
                {
                    throw new IllegalStateException($"Unexpected amqp-value body content type: {value.GetType().Name}");
                }
            }
            else
            {
                throw new IllegalStateException($"Unexpected message body type: {Message.BodySection.GetType().Name}");
            }
        }

        protected override void InitializeEmptyBody()
        {
            base.InitializeEmptyBody();
            list = InitializeEmptyBody(true);
        }

        private List InitializeEmptyBody(bool useSequenceBody)
        {
            List emptyList = new List();
            if (useSequenceBody)
            {
                Message.BodySection = new AmqpSequence {List = emptyList};
            }
            else
            {
                Message.BodySection = new AmqpValue {Value = emptyList};
            }

            return emptyList;
        }

        public virtual bool HasBody() => !IsEmpty;

        public override void ClearBody()
        {
            list.Clear();
            position = 0;
        }

        public override NmsMessage AsMessage()
        {
            return new NmsStreamMessage(this);
        }

        public override INmsMessageFacade Copy()
        {
            AmqpNmsStreamMessageFacade copy = new AmqpNmsStreamMessageFacade();
            CopyInto(copy);
            return copy;
        }
    }
}