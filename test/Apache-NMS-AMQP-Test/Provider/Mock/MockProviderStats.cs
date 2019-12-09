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
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Meta;

namespace NMS.AMQP.Test.Provider.Mock
{
    public class MockProviderStats
    {
        private readonly MockProviderStats parent;
        private readonly Dictionary<Type, int> createResourceCalls = new Dictionary<Type, int>();
        private readonly Dictionary<Type, int> destroyResourceCalls = new Dictionary<Type, int>();
        private readonly Dictionary<Type, int> startResourceCalls = new Dictionary<Type, int>();

        public MockProviderStats(MockProviderStats parent = null)
        {
            this.parent = parent;
        }
        
        public int ConnectionAttempts { get; private set; }
        public int ProvidersCreated { get; private set; }
        public int CloseAttempts { get; private set; }
        public int RecoverCalls { get; set; }

        public int GetCreateResourceCalls<T>() where T : INmsResource => createResourceCalls[typeof(T)];

        public int GetDestroyResourceCalls<T>() where T : INmsResource => destroyResourceCalls[typeof(T)];

        public int GetStartResourceCalls<T>() where T : INmsResource => destroyResourceCalls[typeof(T)];

        public void RecordProviderCreated()
        {
            parent?.RecordProviderCreated();
            ProvidersCreated++;
        }

        public void RecordConnectAttempt()
        {
            parent?.RecordConnectAttempt();
            ConnectionAttempts++;
        }

        public void RecordCloseAttempt()
        {
            parent?.RecordCloseAttempt();
            CloseAttempts++;
        }

        public void RecordRecoverCalls()
        {
            parent?.RecordRecoverCalls();
            RecoverCalls++;
        }

        public void RecordCreateResourceCall(Type type)
        {
            parent?.RecordCreateResourceCall(type);
            if (createResourceCalls.ContainsKey(type))
                createResourceCalls[type]++;
            else
                createResourceCalls[type] = 1;
        }

        public void RecordDestroyResourceCall(Type type)
        {
            parent?.RecordDestroyResourceCall(type);
            if (destroyResourceCalls.ContainsKey(type))
                destroyResourceCalls[type]++;
            else
                destroyResourceCalls[type] = 1;
        }

        public void Reset()
        {
            ConnectionAttempts = 0;
            ProvidersCreated = 0;
            CloseAttempts = 0;
        }
    }
}