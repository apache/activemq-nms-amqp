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
using System.Collections.Specialized;
using Apache.NMS.AMQP.Provider;
using Apache.NMS.AMQP.Util;
using Apache.NMS.Util;
using URISupport = Apache.NMS.Util.URISupport;

namespace NMS.AMQP.Test.Provider.Mock
{
    public class MockProviderFactory : IProviderFactory
    {
        public IProvider CreateProvider(Uri remoteUri)
        {
            MockProviderConfiguration configuration = new MockProviderConfiguration();
            MockRemotePeer remote = MockRemotePeer.Instance;
            remote?.ContextStats.RecordProviderCreated();
            StringDictionary properties = URISupport.ParseParameters(remoteUri);
            StringDictionary mockProperties = PropertyUtil.FilterProperties(properties, "mock.");
            PropertyUtil.SetProperties(configuration, mockProperties);
            MockProvider provider = new MockProvider(remoteUri, configuration, remote);
            return provider;
        }
    }
}