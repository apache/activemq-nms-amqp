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
    using Factory;
    using Util;
    using Util.Types;
    using Util.Types.Map.AMQP;
    class AMQPMapMessageCloak : AMQPMessageCloak, IMapMessageCloak
    {
        private IPrimitiveMap map = null;
        private Map amqpmap = null;
        

        internal AMQPMapMessageCloak(Connection conn) : base(conn)
        {
            InitializeMapBody();
        }

        internal AMQPMapMessageCloak(MessageConsumer c, Amqp.Message msg) : base(c, msg)
        {
            InitializeMapBody();
        }

        internal override byte JMSMessageType { get { return MessageSupport.JMS_TYPE_MAP; } }

        private void InitializeMapBody()
        {
            if (message.BodySection == null)
            {
                amqpmap = new Map();
                map = new AMQPValueMap(amqpmap);
                AmqpValue val = new AmqpValue();
                val.Value = amqpmap;
                message.BodySection = val;
            }
            else 
            {
                if (message.BodySection is AmqpValue)
                {
                    object obj = (message.BodySection as AmqpValue).Value;
                    if (obj == null)
                    {
                        amqpmap = new Map();
                        map = new AMQPValueMap(amqpmap);
                        (message.BodySection as AmqpValue).Value = amqpmap;
                    }
                    else if (obj is Map)
                    {
                        amqpmap = obj as Map;
                        map = new AMQPValueMap(amqpmap);
                    }
                    else
                    {
                        throw new NMSException(string.Format("Invalid message body value type. Type: {0}.", obj.GetType().Name));
                    }
                }
                else
                {
                    throw new NMSException("Invalid message body type.");
                }
            }
            
        }

        IPrimitiveMap IMapMessageCloak.Map
        {
            get
            {
                return map;
            }
        }

        IMapMessageCloak IMapMessageCloak.Copy()
        {
            IMapMessageCloak copy = new AMQPMapMessageCloak(Connection);
            CopyInto(copy);
            return copy;
        }

        protected override void CopyInto(IMessageCloak msg)
        {
            base.CopyInto(msg);
            IPrimitiveMap copy = (msg as IMapMessageCloak).Map;
            foreach (string key in this.map.Keys)
            {
                object value = map[key];
                if (value != null)
                {
                    Type valType = value.GetType();
                    if (valType.IsPrimitive)
                    {
                        // value copy primitive value
                        copy[key] = value;
                    }
                    else if (valType.IsArray && valType.Equals(typeof(byte[])))
                    {
                        // use IPrimitive map SetBytes for most common implementation this is a deep copy.
                        byte[] original = value as byte[];
                        copy.SetBytes(key, original);
                    }
                    else if (valType.Equals(typeof(IDictionary)) || valType.Equals(typeof(Amqp.Types.Map)))
                    {
                        // reference copy
                        copy.SetDictionary(key, value as IDictionary);
                    }
                    else if (valType.Equals(typeof(IList)) || valType.Equals(typeof(Amqp.Types.List)))
                    {
                        // reference copy
                        copy.SetList(key, value as IList);
                    }
                    else
                    {
                        copy[key] = value;
                    }
                }
                else
                {
                    copy[key] = value;
                }
                
            }
        }

        public override string ToString()
        {
            string result = base.ToString();
            if(this.map != null)
            {
                result +=string.Format("\nMessage Body: {0}\n", ConversionSupport.ToString(this.map));
            }
            return result;
        }

    }
    
}
