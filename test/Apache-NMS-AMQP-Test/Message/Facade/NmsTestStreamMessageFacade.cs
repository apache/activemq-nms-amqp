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

using Amqp.Types;
using Apache.NMS;
using Apache.NMS.AMQP.Message.Facade;

namespace NMS.AMQP.Test.Message.Facade
{
    public class NmsTestStreamMessageFacade : NmsTestMessageFacade, INmsStreamMessageFacade
    {
        private readonly List stream = new List();
        private int index = -1;

        public object Peek()
        {
            if (stream.Count == 0 || index + 1 >= stream.Count)
                throw new MessageEOFException("Attempted to read past the end of the stream");

            return stream[index + 1];
        }

        public void Pop()
        {
            if (stream.Count == 0 || index + 1 >= stream.Count)
                throw new MessageEOFException("Attempted to read past the end of the stream");

            index++;
        }

        public void Reset()
        {
            index = -1;
        }

        public void Put(object value)
        {
            stream.Add(value);
        }

        public override void ClearBody()
        {
            stream.Clear();
            index = -1;
        }
    }
}