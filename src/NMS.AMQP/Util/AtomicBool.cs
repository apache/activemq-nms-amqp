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

using System.Threading;

namespace Apache.NMS.AMQP.Util
{
    internal class AtomicBool
    {
        private int value;

        public bool Value => value == 1;

        public AtomicBool(bool initialValue = false)
        {
            value = initialValue ? 1 : 0;
        }

        public bool CompareAndSet(bool expect, bool update)
        {
            int e = expect ? 1 : 0;
            int u = update ? 1 : 0;
            return e == Interlocked.CompareExchange(ref value, u, e);
        }

        public void Set(bool newValue)
        {
            Interlocked.Exchange(ref value, newValue ? 1 : 0);
        }

        public static implicit operator bool(AtomicBool atomicBool)
        {
            return atomicBool.Value;
        }
    }
}