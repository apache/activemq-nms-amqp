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
using System.IO;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp.Message
{
    public class AmqpNmsObjectMessageFacade : AmqpNmsMessageFacade, INmsObjectMessageFacade
    {
        private IAmqpObjectTypeDelegate typeDelegate;

        public IAmqpObjectTypeDelegate Delegate => typeDelegate;

        public object Body
        {
            get => Delegate.Object;
            set => Delegate.Object = value;
        }

        public override sbyte? JmsMsgType => typeDelegate is AmqpTypedObjectDelegate ? (sbyte?) MessageSupport.JMS_TYPE_OBJ : null;

        public override void OnSend(TimeSpan producerTtl)
        {
            base.OnSend(producerTtl);
            Delegate.OnSend();
        }

        public override void ClearBody()
        {
            try
            {
                Body = null;
            }
            catch (IOException e)
            {
            }
        }

        public override NmsMessage AsMessage()
        {
            return new NmsObjectMessage(this);
        }

        public override void Initialize(IAmqpConnection connection)
        {
            base.Initialize(connection);
            InitSerializer(connection.ObjectMessageUsesAmqpTypes);
        }

        public override void Initialize(IAmqpConsumer consumer, global::Amqp.Message message)
        {
            base.Initialize(consumer, message);
            bool dotnetSerialized = MessageSupport.SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE.Equals(ContentType);
            InitSerializer(!dotnetSerialized);
        }

        private void InitSerializer(bool useAmqpTypes)
        {
            if (!useAmqpTypes)
            {
                typeDelegate = new AmqpSerializedObjectDelegate(this);
            }
            else
            {
                typeDelegate = new AmqpTypedObjectDelegate(this);
            }
        }

        public override INmsMessageFacade Copy()
        {
            AmqpNmsObjectMessageFacade copy = new AmqpNmsObjectMessageFacade();
            CopyInto(copy);
            copy.typeDelegate = typeDelegate;
            return copy;
        }
    }
}