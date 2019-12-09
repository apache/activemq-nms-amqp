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
using System.Text;
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Message.Facade;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp.Message
{
    public class AmqpNmsTextMessageFacade : AmqpNmsMessageFacade, INmsTextMessageFacade
    {
        public override NmsMessage AsMessage()
        {
            return new NmsTextMessage(this);
        }

        public string Text
        {
            get => GetTextFromBody();
            set => SetTextBody(value);
        }

        private string GetTextFromBody()
        {
            RestrictedDescribed body = Message.BodySection;
            if (body == null)
                return null;
            if (body is Data data)
                return DecodeBinaryBody(data.Binary);
            if (body is AmqpValue amqpValue)
            {
                object value = amqpValue.Value;
                if (value == null)
                    return null;
                if (value is byte[] bytes)
                    return DecodeBinaryBody(bytes);
                if (value is string text)
                    return text;
                throw new IllegalStateException("Unexpected Amqp value content-type: " + value.GetType().FullName);
            }

            throw new IllegalStateException("Unexpected body content-type: " + body.GetType().FullName);
        }

        private void SetTextBody(string value)
        {
            AmqpValue amqpValue = new AmqpValue {Value = value};
            Message.BodySection = amqpValue;
        }

        public override void ClearBody()
        {
            SetTextBody(null);
        }

        public virtual bool HasBody()
        {
            try
            {
                return Text != null;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public override sbyte? JmsMsgType => MessageSupport.JMS_TYPE_TXT;

        private static string DecodeBinaryBody(byte[] body)
        {
            string result = string.Empty;
            if (body != null && body.Length > 0)
            {
                result = Encoding.UTF8.GetString(body);
            }

            return result;
        }

        public override INmsMessageFacade Copy()
        {
            AmqpNmsTextMessageFacade copy = new AmqpNmsTextMessageFacade();
            CopyInto(copy);
            return copy;
        }
    }
}