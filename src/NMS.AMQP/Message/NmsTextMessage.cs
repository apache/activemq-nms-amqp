﻿/*
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
using Apache.NMS.AMQP.Message.Facade;

namespace Apache.NMS.AMQP.Message
{
    public class NmsTextMessage : NmsMessage, ITextMessage
    {
        private readonly INmsTextMessageFacade facade;

        public NmsTextMessage(INmsTextMessageFacade facade) : base(facade)
        {
            this.facade = facade;
        }

        public string Text
        {
            get => facade.Text;
            set
            {
                CheckReadOnlyBody();
                facade.Text = value;
            }
        }

        public override string ToString()
        {
            return "NmsTextMessage { " + Text + " }";
        }
        
        public override NmsMessage Copy()
        {
            NmsTextMessage copy = new NmsTextMessage(facade.Copy() as INmsTextMessageFacade);
            CopyInto(copy);
            return copy;
        }
        
        public override bool IsBodyAssignableTo(Type type)
        {
            return !facade.HasBody() || type.IsAssignableFrom(typeof(string));
        }
        
        protected override T DoGetBody<T>()
        {
            object o = Text;
            return (T) o;
        }
    }
}