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
using System.Collections;
using System.Collections.Generic;
using NMS.AMQP;
using NMS.AMQP.Util;
using NMS.AMQP.Message;
using Apache.NMS;

namespace NMS.AMQP.Util.Types.Queue
{
    internal interface IMessageDelivery
    {
        Message.Message Message { get; }

        MsgPriority Priority { get; }

        int DeliveryCount { get; }
        bool EnqueueFirst { get; }
    }

    internal interface IMessageQueue : IStartable, IStoppable, ICollection
    {

        void Enqueue(IMessageDelivery message);

        void EnqueueFirst(IMessageDelivery message);

        IMessageDelivery Dequeue();

        IMessageDelivery Dequeue(int timeout);

        IMessageDelivery DequeueNoWait();

        IMessageDelivery Peek();

        IList<IMessageDelivery> RemoveAll();

        void Clear();
        
        bool IsEmpty { get; }

        bool IsClosed { get; }

        void Close();

    }
}
