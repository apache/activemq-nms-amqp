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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.NMS;
using Apache.NMS.Util;
using Amqp.Types;
using Amqp.Framing;

namespace NMS.AMQP.Message.AMQP
{
    using Cloak;
    using Util;
    using Factory;

    class AMQPTextMessageCloak : AMQPMessageCloak, ITextMessageCloak
    {
        #region Constructor

        internal AMQPTextMessageCloak(Connection c) : base(c) {}

        internal AMQPTextMessageCloak(MessageConsumer mc, Amqp.Message msg) : base(mc, msg) {}

        #endregion

        internal override byte JMSMessageType
        {
            get
            {
                return MessageSupport.JMS_TYPE_TXT;
            }
        }

        public string Text
        {
            get
            {
                return GetTextFromBody();
            }

            set
            {
                AmqpValue val = new AmqpValue();
                val.Value = value;
                this.message.BodySection = val;
            }
        }
        
        ITextMessageCloak ITextMessageCloak.Copy()
        {
            ITextMessageCloak tcloak = new AMQPTextMessageCloak(connection);
            CopyInto(tcloak);
            return tcloak;
        }
        
        protected override void CopyInto(IMessageCloak msg)
        {
            base.CopyInto(msg);
            (msg as ITextMessageCloak).Text = Text;
        }

        private static string DecodeBinaryBody(byte[] body)
        {
            string result = string.Empty;
            if(body != null && body.Length > 0)
            {
                result = Encoding.UTF8.GetString(body);
            }
            return result;
        }

        private string GetTextFromBody()
        {
            string result = string.Empty;
            RestrictedDescribed body = this.message.BodySection;
            if(body == null)
            {
                return result;
            }
            else if (body is Data)
            {
                byte[] data = (body as Data).Binary;
                result = DecodeBinaryBody(data);
            }
            else if(body is AmqpValue)
            {
                object value = (body as AmqpValue).Value;
                if(value == null)
                {
                    return result;
                }
                else if (value is byte[])
                {
                    result = DecodeBinaryBody(value as byte[]);
                }
                else if (value is string)
                {
                    result = value as string;
                }
                else
                {
                    throw new IllegalStateException("Unexpected Amqp value content-type: " + value.GetType().FullName);
                }
            }
            else
            {
                throw new IllegalStateException("Unexpected body content-type: " + body.GetType().FullName);
            }


            return result;
        }
        public override string ToString()
        {
            string result = base.ToString();
            if (this.Text != null)
            {
                result += string.Format("\nMessage Body: {0}\n", Text);
            }
            return result;
        }

    }
}
