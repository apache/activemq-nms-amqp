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
using System.Collections.Concurrent;
using System.Collections.Generic;
using Apache.NMS.AMQP.Provider.Amqp;
using Apache.NMS.AMQP.Provider.Failover;

namespace Apache.NMS.AMQP.Provider
{
    public static class ProviderFactory
    {
        private static readonly ConcurrentDictionary<string, IProviderFactory> CachedFactories =
            new ConcurrentDictionary<string, IProviderFactory>(new[]
            {
                new KeyValuePair<string, IProviderFactory>("amqp", new AmqpProviderFactory()), 
                new KeyValuePair<string, IProviderFactory>("amqps", new AmqpProviderFactory()), 
                new KeyValuePair<string, IProviderFactory>("failover", new FailoverProviderFactory()), 
            }, StringComparer.InvariantCultureIgnoreCase);

        public static IProvider Create(Uri remoteUri)
        {
            if (CachedFactories.TryGetValue(remoteUri.Scheme, out var factory))
                return factory.CreateProvider(remoteUri);

            throw new ArgumentException($"Failed to create Provider instance for {remoteUri.Scheme}");
        }

        public static void RegisterProviderFactory(string scheme, IProviderFactory factory)
        {
            CachedFactories.TryAdd(scheme, factory);
        }
    }
}