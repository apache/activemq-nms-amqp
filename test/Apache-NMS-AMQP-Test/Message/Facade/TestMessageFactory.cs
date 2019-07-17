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

using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Provider.Amqp.Message;

namespace NMS.AMQP.Test.Message.Facade
{
    public class TestMessageFactory : INmsMessageFactory
    {
        public NmsMessage CreateMessage()
        {
            return new NmsMessage(new NmsTestMessageFacade());
        }

        public NmsTextMessage CreateTextMessage()
        {
            return CreateTextMessage(null);
        }

        public NmsTextMessage CreateTextMessage(string payload)
        {
            return new NmsTextMessage(new NmsTestTextMessageFacade()) { Text = payload };
        }

        public NmsStreamMessage CreateStreamMessage()
        {
            return new NmsStreamMessage(new NmsTestStreamMessageFacade());
        }

        public NmsBytesMessage CreateBytesMessage()
        {
            return new NmsBytesMessage(new NmsTestBytesMessageFacade());
        }

        public NmsBytesMessage CreateBytesMessage(byte[] body)
        {
            NmsBytesMessage bytesMessage = CreateBytesMessage();
            bytesMessage.Content = body;
            return bytesMessage;
        }

        public NmsMapMessage CreateMapMessage()
        {
            return new NmsMapMessage(new NmsTestMapMessageFacade());
        }

        public NmsObjectMessage CreateObjectMessage()
        {
            return new NmsObjectMessage(new NmsTestObjectMessageFacade());
        }

        public NmsObjectMessage CreateObjectMessage(object body)
        {
            NmsObjectMessage objectMessage = CreateObjectMessage();
            objectMessage.Body = body;
            return objectMessage;
        }
    }
}