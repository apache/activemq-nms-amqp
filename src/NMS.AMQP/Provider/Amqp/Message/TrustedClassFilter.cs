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
using System.Reflection;
using System.Runtime.Serialization;
using Apache.NMS.AMQP.Policies;

namespace Apache.NMS.AMQP.Provider.Amqp.Message
{
    internal class TrustedClassFilter : SerializationBinder
    {
        private readonly INmsDeserializationPolicy deserializationPolicy;
        private readonly IDestination destination;

        public TrustedClassFilter(INmsDeserializationPolicy deserializationPolicy, IDestination destination)
        {
            this.deserializationPolicy = deserializationPolicy;
            this.destination = destination;
        }
        
        public override Type BindToType(string assemblyName, string typeName)
        {
            var name = new AssemblyName(assemblyName);
            var assembly = Assembly.Load(name);
            var type = FormatterServices.GetTypeFromAssembly(assembly, typeName);
            if (deserializationPolicy.IsTrustedType(destination, type))
            {
                return type;
            }

            var message = $"Forbidden {type.FullName}! " +
                          "This type is not trusted to be deserialized under the current configuration. " +
                          "Please refer to the documentation for more information on how to configure trusted types.";
            throw new SerializationException(message);
        }
    }
}