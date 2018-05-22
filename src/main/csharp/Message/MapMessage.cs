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
using NMS.AMQP.Message.Cloak;

namespace NMS.AMQP.Message
{
    /// <summary>
    /// NMS.AMQP.Message.MapMessage inherits from NMS.AMQP.Message.Message that implements the Apache.NMS.IMapMessage interface.
    /// NMS.AMQP.Message.MapMessage uses the NMS.AMQP.Message.Cloak.IMapMessageCloak interface to detach from the underlying AMQP 1.0 engine.
    /// </summary>
    class MapMessage : Message, IMapMessage
    {
        new private readonly IMapMessageCloak cloak;
        private PrimitiveMapInterceptor map;

        internal MapMessage(IMapMessageCloak message) : base(message)
        {
            cloak = message;
        }

        public override bool IsReadOnly
        {
            get { return base.IsReadOnly; }
            internal set
            {
                if (map != null)
                {
                    map.ReadOnly = value;
                }
                base.IsReadOnly = value;
            }
        }

        public IPrimitiveMap Body
        {
            get
            {
                if(map == null)
                {
                    map = new PrimitiveMapInterceptor(this, cloak.Map, IsReadOnly, true);
                }
                return map;
            }
        }

        internal override Message Copy()
        {
            return new MapMessage(cloak.Copy());
        }
    }
}
