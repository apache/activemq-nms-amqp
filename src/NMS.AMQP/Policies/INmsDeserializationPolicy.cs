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

namespace Apache.NMS.AMQP.Policies
{
    /// <summary>
    /// Defines the interface for a policy that controls the permissible message content 
    /// during the deserialization of the body of an incoming <see cref="IObjectMessage"/>.
    /// </summary>
    public interface INmsDeserializationPolicy
    {
        /// <summary>
        /// Determines if the given class is a trusted type that can be deserialized by the client.
        /// </summary>
        /// <param name="destination">The Destination for the message containing the type to be deserialized.</param>
        /// <param name="type">The type of the object that is about to be read.</param>
        /// <returns>True if the type is trusted, otherwise false.</returns>
        bool IsTrustedType(IDestination destination, Type type);
        
        /// <summary>
        /// Makes a thread-safe copy of the INmsDeserializationPolicy object.
        /// </summary>
        /// <returns>A copy of INmsDeserializationPolicy object.</returns>
        INmsDeserializationPolicy Clone();
    }
}