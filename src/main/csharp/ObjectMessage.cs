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

using System.IO;

#if !(PocketPC||NETCF||NETCF_2_0)
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
#endif

// TODO: Any support

namespace Apache.NMS.Amqp
{
    public class ObjectMessage : BaseMessage, IObjectMessage
    {
        private object body;
#if !(PocketPC||NETCF||NETCF_2_0)
        private IFormatter formatter;
#endif

        public ObjectMessage()
        {
        }

        public ObjectMessage(object body)
        {
            this.body = body;
        }

        public object Body
        {
            get
            {
#if !(PocketPC||NETCF||NETCF_2_0)
                if(body == null)
                {
                    body = Formatter.Deserialize(new MemoryStream(Content));
                }
#else
#endif
                return body;
            }

            set
            {
#if !(PocketPC||NETCF||NETCF_2_0)
                body = value;
#else
                throw new NotImplementedException();
#endif
            }
        }


#if !(PocketPC||NETCF||NETCF_2_0)
        public IFormatter Formatter
        {
            get
            {
                if(formatter == null)
                {
                    formatter = new BinaryFormatter();
                }
                return formatter;
            }

            set
            {
                formatter = value;
            }
        }

#endif
    }
}
