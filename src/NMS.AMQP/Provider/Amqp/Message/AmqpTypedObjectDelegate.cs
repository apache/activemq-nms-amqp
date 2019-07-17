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
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS.AMQP.Util.Types;

namespace Apache.NMS.AMQP.Provider.Amqp.Message
{
    public class AmqpTypedObjectDelegate : IAmqpObjectTypeDelegate
    {
        public static readonly AmqpValue NULL_OBJECT_BODY = new AmqpValue {Value = null};

        private readonly AmqpNmsObjectMessageFacade facade;

        public AmqpTypedObjectDelegate(AmqpNmsObjectMessageFacade facade)
        {
            this.facade = facade;
        }

        public object Object
        {
            get
            {
                RestrictedDescribed body = null;
                if (facade.Message.BodySection != null)
                {
                    global::Amqp.Message copy = new global::Amqp.Message(facade.Message.Body);
                    ByteBuffer encodedBody = copy.Encode();
                    global::Amqp.Message message = global::Amqp.Message.Decode(encodedBody);
                    body = message.BodySection;
                }
                
                switch (body)
                {
                    case null:
                        return null;
                    case AmqpValue value:
                        return value.Value;
                    case Data data:
                        return data.Binary;
                    case AmqpSequence sequence:
                        return sequence.List;
                    default:
                        throw new IllegalStateException("Unexpected body type: " + body.GetType().Name);
                }
            }
            set
            {
                if (value == null)
                {
                    facade.Message.BodySection = NULL_OBJECT_BODY;
                }
                else if (IsNMSObjectTypeSupported(value))
                {
                    object o = null;
                    if (value is IList list)
                        o = ConversionSupport.ListToAmqp(list);
                    else if (value is IPrimitiveMap primitiveMap)
                        o = ConversionSupport.NMSMapToAmqp(primitiveMap);
                    else
                        o = value;

                    // to copy the object being set encode a message then decode and take body
                    global::Amqp.Message copy = new global::Amqp.Message(o);
                    ByteBuffer buffer = copy.Encode();
                    copy = global::Amqp.Message.Decode(buffer);

                    facade.Message.BodySection = new AmqpValue {Value = copy.Body};
                }
                else
                {
                    throw new ArgumentException("Encoding this object type with the AMQP type system is not supported: " + value.GetType().Name);
                }
            }
        }

        private bool IsNMSObjectTypeSupported(object o)
        {
            return ConversionSupport.IsNMSType(o) || o is List || o is Map || o is IPrimitiveMap || o is IList;
        }

        public void OnSend()
        {
            facade.ContentType = null;
            if (facade.Message.BodySection == null)
                facade.Message.BodySection = NULL_OBJECT_BODY;
        }
    }
}