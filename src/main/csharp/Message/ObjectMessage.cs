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
using System.Runtime.Serialization;
using Apache.NMS;
using Apache.NMS.Util;
using NMS.AMQP.Util.Types;

namespace NMS.AMQP.Message
{
    using Cloak;
    class ObjectMessage : Message, IObjectMessage
    {
        protected new readonly IObjectMessageCloak cloak;
        internal ObjectMessage(IObjectMessageCloak message) : base(message)
        {
            this.cloak = message;
        }

        public new byte[] Content
        {
            get
            {
                return cloak.Content;
            }

            set
            {
                
            }
        }

        public object Body
        {
            get
            {
                return cloak.Body;
            }
            set
            {
                
                try
                {
                    cloak.Body = value;
                }
                catch (SerializationException se)
                {
                    throw NMSExceptionSupport.CreateMessageFormatException(se);
                }
                
            }
        }

        internal override Message Copy()
        {
            return new ObjectMessage(this.cloak.Copy());
        }
    }
}
