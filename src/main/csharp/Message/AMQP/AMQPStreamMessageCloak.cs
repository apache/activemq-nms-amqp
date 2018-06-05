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
using System.Collections;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.NMS;
using Apache.NMS.Util;
using Amqp.Framing;
using Amqp.Types;

namespace NMS.AMQP.Message.AMQP
{

    using Cloak;
    using Util;
    using Factory;
    class AMQPStreamMessageCloak : AMQPMessageCloak, IStreamMessageCloak
    {
        private IList list;
        private int position = 0;
        internal AMQPStreamMessageCloak(Connection c):base(c)
        {
            list = InitializeEmptyBody(true);
        }

        internal AMQPStreamMessageCloak(MessageConsumer mc, Amqp.Message msg) : base(mc, msg)
        {
            if (msg.BodySection == null)
            {
                list = InitializeEmptyBody(true);
            }
            else if (msg.BodySection is AmqpSequence)
            {
                IList value = (msg.BodySection as AmqpSequence).List;
                if(value == null)
                {
                    list = InitializeEmptyBody(true);
                }
                else
                {
                    list = value;
                }
            }
            else if (msg.BodySection is AmqpValue)
            {
                object value = (msg.BodySection as AmqpValue).Value;
                if(value == null)
                {
                    list = InitializeEmptyBody(false);
                }
                else if (value is IList)
                {
                    list = value as IList;
                }
                else
                {
                    throw new IllegalStateException("Unexpected amqp-value body content type: " + value.GetType().Name);
                }
            }
            else
            {
                throw new IllegalStateException("Unexpected message body type: " + msg.BodySection.GetType().Name);
            }
        }

        internal override byte JMSMessageType { get { return MessageSupport.JMS_TYPE_STRM; } }

        #region Private Methods

        private List InitializeEmptyBody(bool isSequence)
        {
            List l = new List();
            if (isSequence)
            {
                AmqpSequence seq = new Amqp.Framing.AmqpSequence();
                message.BodySection = seq;
                seq.List = l;
                
            }
            else
            {
                Amqp.Framing.AmqpValue val = new Amqp.Framing.AmqpValue();
                val.Value = l;
                message.BodySection = val;
            }
            return l;
        }

        private bool IsEmpty { get { return list.Count <= 0; } }

        #endregion

        #region IStreamMessageCloak Methods
        public bool HasNext { get { return !IsEmpty && position < list.Count; } }

        public object Peek()
        {
            if(IsEmpty || position >= list.Count)
            {
                throw new EndOfStreamException("Attempt to read past the end of stream");
            }
            object value = list[position];
            if(value != null && value is byte[])
            {
                byte[] binary = value as byte[];
                byte[] bin = new byte[binary.Length];
                binary.CopyTo(bin, 0);
                value = bin;
            }
            return value;
        }

        public void Pop()
        {
            if (IsEmpty || position > list.Count)
            {
                throw new EndOfStreamException("Attempt to read past the end of stream");
            }
            position++;
        }

        public void Put(object value)
        {
            object entry = value;
            if (entry != null && entry is byte[])
            {
                byte[] bin = new byte[(entry as byte[]).Length];
                (entry as byte[]).CopyTo(bin, 0);
                entry = bin;
            }
            if (list.Add(entry) < 0)
            {
                throw NMSExceptionSupport.Create(string.Format("Failed to add {0} to stream.", entry.ToString()), null);
            }
            position++;
        }

        public void Reset()
        {
            position = 0;
        }

        public override void ClearBody()
        {
            base.ClearBody();
            list.Clear();
            position = 0;
        }

        IStreamMessageCloak IStreamMessageCloak.Copy()
        {
            return base.Copy() as IStreamMessageCloak;
        }

        protected override void CopyInto(IMessageCloak msg)
        {
            base.CopyInto(msg);
            if(msg is IStreamMessageCloak)
            {
                IStreamMessageCloak copy = (msg as IStreamMessageCloak);
                
                foreach(object o in list)
                {
                    copy.Put(o);
                }
            }
        }
        #endregion
    }
}
