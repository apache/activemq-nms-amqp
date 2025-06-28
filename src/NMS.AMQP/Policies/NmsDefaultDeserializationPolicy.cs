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

namespace Apache.NMS.AMQP.Policies
{
    /// <summary>
    /// Default implementation of the deserialization policy that can read allow and deny lists of
    /// types/namespaces from the connection URI options.
    ///
    /// The policy reads a default deny list string value (comma separated) from the connection URI options
    /// (nms.deserializationPolicy.deny) which defaults to null which indicates an empty deny list.
    ///
    /// The policy reads a default allow list string value (comma separated) from the connection URI options
    /// (nms.deserializationPolicy.allowList) which defaults to <see cref="CATCH_ALL_WILDCARD"/> which
    /// indicates that all types are allowed.
    ///
    /// The deny list overrides the allow list, entries that could match both are counted as denied.
    ///
    /// If the policy should treat all classes as untrusted, the deny list should be set to <see cref="CATCH_ALL_WILDCARD"/>.
    /// </summary>
    public class NmsDefaultDeserializationPolicy : INmsDeserializationPolicy
    {
        /// <summary>
        /// Value used to indicate that all types should be allowed or denied
        /// </summary>
        public const string CATCH_ALL_WILDCARD = "*";
        
        private IReadOnlyList<string> denyList = [];
        private IReadOnlyList<string> allowList = [CATCH_ALL_WILDCARD];
        
        public bool IsTrustedType(IDestination destination, Type type)
        {
            var typeName = type?.FullName;
            if (typeName == null)
            {
                return false;
            }

            foreach (var denyListEntry in denyList)
            {
                if (CATCH_ALL_WILDCARD == denyListEntry)
                {
                    return false;
                }
                if (IsTypeOrNamespaceMatch(typeName, denyListEntry))
                {
                    return false;
                }
            }

            foreach (var allowListEntry in allowList)
            {
                if (CATCH_ALL_WILDCARD == allowListEntry)
                {
                    return true;
                }
                if (IsTypeOrNamespaceMatch(typeName, allowListEntry))
                {
                    return true;
                }
            }
            
            // Failing outright rejection or allow from above, reject.
            return false;
        }

        private bool IsTypeOrNamespaceMatch(string typeName, string listEntry)
        {
            // Check if type is an exact match of the entry
            if (typeName == listEntry)
            {
                return true;
            }
            
            // Check if the type is from a namespace matching the entry
            var entryLength = listEntry.Length;
            return typeName.Length > entryLength && typeName.StartsWith(listEntry) && '.' == typeName[entryLength];
        }

        public INmsDeserializationPolicy Clone()
        {
            return new NmsDefaultDeserializationPolicy
            {
                allowList = allowList.ToArray(),
                denyList = denyList.ToArray()
            };
        }

        /// <summary>
        /// Gets or sets the deny list on this policy instance.
        /// </summary>
        public string DenyList
        {
            get => string.Join(",", denyList);
            set => denyList = string.IsNullOrWhiteSpace(value) 
                ? Array.Empty<string>() 
                : value.Split(',');
        }

        /// <summary>
        /// Gets or sets the allow list on this policy instance.
        /// </summary>
        public string AllowList
        {
            get => string.Join(",", allowList);
            set => allowList = string.IsNullOrWhiteSpace(value) 
                ? Array.Empty<string>() 
                : value.Split(',');
        }
    }
}