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
using Apache.NMS.AMQP.Message.Factory;

namespace Apache.NMS.AMQP.Message.AMQP
{
    class AMQPMessageTransformation <T> : MessageTransformation where T:ConnectionInfo
    {
        protected readonly Connection connection;
        protected readonly MessageFactory<T> factory;

        public AMQPMessageTransformation(AMQPMessageFactory<T> fact) : base()
        {
            connection = fact.Parent;    
            factory = fact;
        }

        protected override IBytesMessage DoCreateBytesMessage()
        {
            return factory.CreateBytesMessage();
        }

        protected override IMapMessage DoCreateMapMessage()
        {
            return factory.CreateMapMessage();
        }

        protected override IMessage DoCreateMessage()
        {
            return factory.CreateMessage();
        }

        protected override IObjectMessage DoCreateObjectMessage()
        {
            return factory.CreateObjectMessage(null);
        }

        protected override IStreamMessage DoCreateStreamMessage()
        {
            return factory.CreateStreamMessage();
        }

        protected override ITextMessage DoCreateTextMessage()
        {
            return factory.CreateTextMessage();
        }

        protected override void DoPostProcessMessage(IMessage message)
        {
            // nothing for now
        }

        protected override IDestination DoTransformDestination(IDestination destination)
        {
            return DestinationTransformation.Transform(connection, destination);
        }
    }
}
