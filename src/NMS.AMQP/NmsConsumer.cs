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

namespace Apache.NMS.AMQP
{
    public class NmsConsumer : INMSConsumer
    {
        
        private readonly ISession session;
        private readonly NmsMessageConsumer consumer;

        public NmsConsumer(ISession session, NmsMessageConsumer consumer) {
            this.session = session;
            this.consumer = consumer;
        }

        public void Dispose()
        {
            consumer.Dispose();
        }

        public IMessage Receive()
        {
            return consumer.Receive();
        }

        public IMessage Receive(TimeSpan timeout)
        {
            return consumer.Receive(timeout);
        }

        public IMessage ReceiveNoWait()
        {
            return consumer.ReceiveNoWait();
        }

        public T ReceiveBody<T>()
        {
            return consumer.ReceiveBody<T>();
        }

        public T ReceiveBody<T>(TimeSpan timeout)
        {
            return consumer.ReceiveBody<T>(timeout);
        }

        public T ReceiveBodyNoWait<T>()
        {
            return consumer.ReceiveBodyNoWait<T>();
        }

        public void Close()
        {
            consumer.Close();
        }

        public string MessageSelector => consumer.MessageSelector;

        public ConsumerTransformerDelegate ConsumerTransformer
        {
            get => consumer.ConsumerTransformer; 
            set => consumer.ConsumerTransformer = value; 
        }

        event MessageListener INMSConsumer.Listener
        {
            add => ((IMessageConsumer)consumer).Listener += value;
            remove => ((IMessageConsumer)consumer).Listener -= value;
        }
    }
}