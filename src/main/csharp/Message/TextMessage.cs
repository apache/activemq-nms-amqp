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

namespace NMS.AMQP.Message
{
    using Cloak;
    /// <summary>
    /// NMS.AMQP.Message.TextMessage inherits from NMS.AMQP.Message.Message that implements the Apache.NMS.ITextMessage interface.
    /// NMS.AMQP.Message.TextMessage uses the NMS.AMQP.Message.Cloak.ITextMessageCloak interface to detach from the underlying AMQP 1.0 engine.
    /// </summary>
    class TextMessage : Message, ITextMessage
    {
        protected readonly new ITextMessageCloak cloak;

        #region Constructor

        internal TextMessage(ITextMessageCloak cloak) : base(cloak)
        {
            this.cloak = cloak;
        }

        #endregion

        #region ITextMessage Properties

        public string Text
        {
            get
            {
                return cloak.Text;
            }

            set
            {
                FailIfReadOnlyMsgBody();
                cloak.Text = value;
            }
        }

        #endregion

        internal override Message Copy()
        {
            return new TextMessage(this.cloak.Copy());
        }
    }
}
