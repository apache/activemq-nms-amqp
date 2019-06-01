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

using Apache.NMS.AMQP.Message;

namespace Apache.NMS.AMQP.Provider.Amqp.Message
{
    public class AmqpMessageFactory : INmsMessageFactory
    {
        private readonly IAmqpConnection connection;

        public AmqpMessageFactory(IAmqpConnection connection)
        {
            this.connection = connection;
        }

        public NmsMessage CreateMessage()
        {
            AmqpNmsMessageFacade facade = new AmqpNmsMessageFacade();
            facade.Initialize(connection);
            return facade.AsMessage();
        }

        public NmsTextMessage CreateTextMessage()
        {
            AmqpNmsTextMessageFacade facade = new AmqpNmsTextMessageFacade();
            facade.Initialize(connection);
            return facade.AsMessage() as NmsTextMessage;
        }

        public NmsTextMessage CreateTextMessage(string payload)
        {
            NmsTextMessage message = CreateTextMessage();
            message.Text = payload;
            return message;
        }

        public NmsStreamMessage CreateStreamMessage()
        {
            AmqpNmsStreamMessageFacade facade = new AmqpNmsStreamMessageFacade();
            facade.Initialize(connection);
            return facade.AsMessage() as NmsStreamMessage;
        }

        public NmsBytesMessage CreateBytesMessage()
        {
            AmqpNmsBytesMessageFacade facade = new AmqpNmsBytesMessageFacade();
            facade.Initialize(connection);
            return facade.AsMessage() as NmsBytesMessage;
        }

        public NmsBytesMessage CreateBytesMessage(byte[] body)
        {
            NmsBytesMessage bytesMessage = CreateBytesMessage();
            bytesMessage.Content = body;
            return bytesMessage;
        }

        public NmsMapMessage CreateMapMessage()
        {
            AmqpNmsMapMessageFacade facade = new AmqpNmsMapMessageFacade();
            facade.Initialize(connection);
            return facade.AsMessage() as NmsMapMessage;
        }

        public NmsObjectMessage CreateObjectMessage()
        {
            AmqpNmsObjectMessageFacade facade = new AmqpNmsObjectMessageFacade();
            facade.Initialize(connection);
            return facade.AsMessage() as NmsObjectMessage;
        }

        public NmsObjectMessage CreateObjectMessage(object body)
        {
            NmsObjectMessage objectMessage = CreateObjectMessage();
            objectMessage.Body = body;
            return objectMessage;
        }
    }
}