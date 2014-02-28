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

using System.Collections.Generic;
using Apache.NMS.Util;

namespace Apache.NMS.Amqp
{
    public class MapMessage : BaseMessage, IMapMessage
    {
        private IPrimitiveMap body = new PrimitiveMap();

        public override object Clone()
        {
            MapMessage mm = (MapMessage)base.Clone();
            DefaultMessageConverter msgConverter = new DefaultMessageConverter();
            Dictionary<string, object> properties = new Dictionary<string, object>();
            properties = msgConverter.FromNmsPrimitiveMap((PrimitiveMap)body);
            msgConverter.SetNmsPrimitiveMap(mm.body, properties);
            return (MapMessage)mm;
        }

        public override void ClearBody()
        {
            base.ClearBody();

            body.Clear();
        }

        public IPrimitiveMap Body
        {
            get { return body; }
            set { body = value; }
        }
    }
}

