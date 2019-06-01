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
using Apache.NMS.AMQP.Message.Facade;

namespace Apache.NMS.AMQP.Message
{
    public class NmsObjectMessage : NmsMessage, IObjectMessage
    {
        private readonly INmsObjectMessageFacade facade;

        public NmsObjectMessage(INmsObjectMessageFacade facade) : base(facade)
        {
            this.facade = facade;
        }

        public object Body
        {
            get => this.facade.Body;
            set
            {
                CheckReadOnlyBody();
                try
                {
                    this.facade.Body = value;
                }
                catch (Exception e)
                {
                    throw new MessageFormatException("Failed to serialize object", e);
                }
            }
        }
        
        public override string ToString()
        {
            return $"NmsObjectMessage {{ {Facade} }}";
        }
        
        public override NmsMessage Copy()
        {
            NmsObjectMessage copy = new NmsObjectMessage(facade.Copy() as INmsObjectMessageFacade);
            CopyInto(copy);
            return copy;
        }
    }
}