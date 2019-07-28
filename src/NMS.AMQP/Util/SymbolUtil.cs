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
using System.Text;
using System.Threading.Tasks;
using Amqp.Types;
using Apache.NMS;

namespace Apache.NMS.AMQP.Util
{
    /// <summary>
    /// Utility class for Amqp.Symbol handling from Strings and Constants.
    /// </summary>
    public class SymbolUtil
    {
        
        // Open Frame Property Symbols
        public readonly static Symbol CONNECTION_ESTABLISH_FAILED = new Symbol("amqp:connection-establishment-failed");
        public readonly static Symbol CONNECTION_PROPERTY_TOPIC_PREFIX = new Symbol("topic-prefix");
        public readonly static Symbol CONNECTION_PROPERTY_QUEUE_PREFIX = new Symbol("queue-prefix");

        // Symbols used for connection capabilities
        public static readonly Symbol OPEN_CAPABILITY_SOLE_CONNECTION_FOR_CONTAINER = new Symbol("sole-connection-for-container");
        public static readonly Symbol OPEN_CAPABILITY_ANONYMOUS_RELAY = new Symbol("ANONYMOUS-RELAY");
        public static readonly Symbol OPEN_CAPABILITY_DELAYED_DELIVERY = new Symbol("DELAYED_DELIVERY");

        // Attach Frame 
        public readonly static Symbol ATTACH_EXPIRY_POLICY_LINK_DETACH = new Symbol("link-detach");
        public readonly static Symbol ATTACH_EXPIRY_POLICY_SESSION_END = new Symbol("session-end");
        public readonly static Symbol ATTACH_EXPIRY_POLICY_NEVER = new Symbol("never");
        public readonly static Symbol ATTACH_DISTRIBUTION_MODE_COPY = new Symbol("copy");
        public readonly static Symbol ATTACH_CAPABILITIES_QUEUE = new Symbol("queue");
        public readonly static Symbol ATTACH_CAPABILITIES_TOPIC = new Symbol("topic");
        public readonly static Symbol ATTACH_CAPABILITIES_TEMP_TOPIC = new Symbol("temporary-topic");
        public readonly static Symbol ATTACH_CAPABILITIES_TEMP_QUEUE = new Symbol("temporary-queue");
        public readonly static Symbol ATTACH_DYNAMIC_NODE_PROPERTY_LIFETIME_POLICY = new Symbol("lifetime-policy");
        public readonly static Symbol ATTACH_FILTER_NO_LOCAL = new Symbol("no-local");
        public readonly static Symbol ATTACH_FILTER_SELECTOR = new Symbol("jms-selector");
        public readonly static Symbol ATTACH_OUTCOME_ACCEPTED = new Symbol(MessageSupport.ACCEPTED_INSTANCE.Descriptor.Name);
        public readonly static Symbol ATTACH_OUTCOME_RELEASED = new Symbol(MessageSupport.RELEASED_INSTANCE.Descriptor.Name);
        public readonly static Symbol ATTACH_OUTCOME_REJECTED = new Symbol(MessageSupport.REJECTED_INSTANCE.Descriptor.Name);
        public readonly static Symbol ATTACH_OUTCOME_MODIFIED = new Symbol(MessageSupport.MODIFIED_INSTANCE.Descriptor.Name);

        //JMS Message Annotation Symbols
        public static readonly Symbol JMSX_OPT_MSG_TYPE = new Symbol("x-opt-jms-msg-type");
        public static readonly Symbol JMSX_OPT_DEST = new Symbol("x-opt-jms-dest");
        public static readonly Symbol JMSX_OPT_REPLY_TO = new Symbol("x-opt-jms-reply-to");

        // Frame Property Value
        public readonly static Symbol BOOLEAN_TRUE = new Symbol("true");
        public readonly static Symbol BOOLEAN_FALSE = new Symbol("false");
        public readonly static Symbol DELETE_ON_CLOSE = new Symbol("delete-on-close");

        // Message Content-Type Symbols
        public static readonly Symbol OCTET_STREAM_CONTENT_TYPE = new Symbol(MessageSupport.OCTET_STREAM_CONTENT_TYPE);
        public static readonly Symbol SERIALIZED_JAVA_OBJECT_CONTENT_TYPE = new Symbol(MessageSupport.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);
        public static readonly Symbol SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE = new Symbol(MessageSupport.SERIALIZED_DOTNET_OBJECT_CONTENT_TYPE);
        
        public static readonly Symbol SHARED = new Symbol("shared");
        public static readonly Symbol GLOBAL = new Symbol("global");

        public static bool FieldsHasSymbol(Fields fields, Symbol symbol)
        {
            return (fields!=null && symbol!=null) ? fields.ContainsKey(symbol) : false;
        }

        public static bool CheckAndCompareFields(Fields fields, Symbol key, Symbol expected)
        {
            return (FieldsHasSymbol(fields, key) && expected!=null) ? fields[key].ToString().Equals(expected.ToString()) : false;
        }

        public static Symbol GetSymbolFromFields(Fields fields, Symbol key)
        {
            return (FieldsHasSymbol(fields, key)) ? fields[key] as Symbol : null;
        }

        public static object GetFromFields(Fields fields, Symbol key)
        {
            return (FieldsHasSymbol(fields, key)) ? fields[key] : null;
        }

        public static Symbol GetTerminusCapabilitiesForDestination(IDestination destination)
        {
            if (destination.IsQueue)
            {
                if (destination.IsTemporary)
                {
                    return ATTACH_CAPABILITIES_TEMP_QUEUE;
                }
                else
                {
                    return ATTACH_CAPABILITIES_QUEUE;
                }
            }
            else if(destination.IsTopic)
            {
                if (destination.IsTemporary)
                {
                    return ATTACH_CAPABILITIES_TEMP_TOPIC;
                }
                else
                {
                    return ATTACH_CAPABILITIES_TOPIC;
                }
            }
            // unknown destination type...
            return null;
        }

    }
}
