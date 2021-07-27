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

using System.Text;
using Apache.NMS.AMQP.Provider.Amqp;

namespace Apache.NMS.AMQP.Message
{
    public class DefaultMessageIdBuilder : INmsMessageIdBuilder
    {
        private readonly StringBuilder builder = new StringBuilder();
        private int idPrefixLength = -1;

        public object CreateMessageId(string producerId, long messageSequence)
        {
            if (idPrefixLength < 0)
            {
                Initialize(producerId);
            }
            
            builder.Length = idPrefixLength;
            builder.Append(messageSequence);
            
            return builder.ToString();
        }

        private void Initialize(string producerId)
        {
            if (!AmqpMessageIdHelper.HasMessageIdPrefix(producerId))
            {
                builder.Append(AmqpMessageIdHelper.NMS_ID_PREFIX);
            }
            builder.Append(producerId).Append("-");
            idPrefixLength = builder.Length;
        }
    }
}