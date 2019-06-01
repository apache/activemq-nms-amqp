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
using Apache.NMS.AMQP.Transport;
using Apache.NMS.AMQP.Util;
using URISupport = Apache.NMS.Util.URISupport;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    internal class AmqpProviderFactory : IProviderFactory
    {
        public IProvider CreateProvider(Uri remoteUri)
        {
            ITransportContext transportContext = TransportContextFactory.CreateTransportContext(remoteUri);
            AmqpProvider amqpProvider = new AmqpProvider(remoteUri, transportContext);

            StringDictionary properties = URISupport.ParseQuery(remoteUri.Query);
            StringDictionary filteredProperties = PropertyUtil.FilterProperties(properties, "amqp.");
            PropertyUtil.SetProperties(amqpProvider, filteredProperties);
            
            return amqpProvider;
        }
    }
}