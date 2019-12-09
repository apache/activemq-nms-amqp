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

using Amqp.Framing;
using Amqp.Types;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.AMQP.Util;
using Apache.NMS.AMQP.Util.Types.Map.AMQP;

namespace Apache.NMS.AMQP.Provider.Amqp.Message
{
    public class AmqpNmsMapMessageFacade : AmqpNmsMessageFacade, INmsMapMessageFacade
    {
        private AMQPValueMap map;
        public IPrimitiveMap Map => map;

        public override sbyte? JmsMsgType => MessageSupport.JMS_TYPE_MAP;

        protected override void InitializeEmptyBody()
        {
            Map amqpMap = new Map();
            AmqpValue val = new AmqpValue {Value = amqpMap};
            map = new AMQPValueMap(amqpMap);
            Message.BodySection = val;
        }

        protected override void InitializeBody()
        {
            if (Message.BodySection is null)
            {
                InitializeEmptyBody();
            }
            else if (Message.BodySection is AmqpValue amqpValue)
            {
                object obj = amqpValue.Value;
                if (obj == null)
                {
                    InitializeEmptyBody();
                }
                else if (obj is Map amqpMap)
                {
                    map = new AMQPValueMap(amqpMap);
                }
                else
                {
                    throw new IllegalStateException($"Unexpected message body value type. Type: {obj.GetType().Name}.");
                }
            }
            else
            {
                throw new IllegalStateException("Unexpected message body type.");
            }
        }

        public virtual bool HasBody()
        {
            return Map.Count > 0;
        }

        public override void ClearBody()
        {
            map.Clear();
        }

        public override NmsMessage AsMessage()
        {
            return new NmsMapMessage(this);
        }

        public override INmsMessageFacade Copy()
        {
            AmqpNmsMapMessageFacade copy = new AmqpNmsMapMessageFacade();
            CopyInto(copy);
            return copy;
        }
    }
}