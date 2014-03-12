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
using Org.Apache.Qpid.Messaging;

// Typedef for options map
using OptionsMap = System.Collections.Generic.Dictionary<System.String, System.Object>;

namespace Apache.NMS.Amqp
{

    /// <summary>
    /// Summary description for Destination.
    /// 
    /// A Destination in Amqp is contained in a Qpid.Messaging.Address.
    /// Destination constructors:
    /// * from strings in the form:
    ///   name[/subject];[{keyword:value, ...}]
    ///   Where:
    ///     name - is the simple name of the queue or topic
    ///     subject - is the associated subject
    ///     options are supplied in a map of keyword:value pairs
    /// * from (string, string, OptionsMap)
    /// * from other Destinations of the same type
    /// Properties:
    ///   Path    - the full ToStr() value of the Messaging.Address.
    ///   Name    - Messaging.Address name
    ///   Subject - Messaging.Address subject
    ///   Options - Messaging.Address OptionsMap dictionary
    ///   Address - the whole Messaging.Address
    /// See http://qpid.apache.org/releases/qpid-0.24/programming/book/
    /// for more information about the Qpid Messaging API and Addresses
    /// </summary>
    public abstract class Destination : IDestination
    {
        private Address qpidAddress = null;

        /**
         * The Default Constructor
         */
        protected Destination()
        {
            qpidAddress = new Address();
        }

        /**
         * Construct the Destination with a defined physical name;
         *
         * @param name
         */
        protected Destination(String name)
        {
            qpidAddress = new Address(name);
        }

        /**
         * Construct the Destination with name, subject, and options
         * 
         * @param name
         * @param subject
         * @param options dictionary
         */
        protected Destination(String name, String subject, OptionsMap options)
        {
            qpidAddress = new Address(name, subject, options);
        }


        /**
         * Construct the Destination with name, subject, options, and type
         * 
         * @param name
         * @param subject
         * @param options dictionary
         * @param type
         */
        protected Destination(String name, String subject, OptionsMap options, String type)
        {
            qpidAddress = new Address(name, subject, options, type);
        }


        protected Destination(Destination other)
        {
            qpidAddress = new Org.Apache.Qpid.Messaging.Address(other.Address);
        }

		/**
		 * Dispose of the destination object.
		 */
		public void Dispose()
		{
		}

        /**
         * Path property
         * get - returns Messaging.Address full string
         * set - creates new Messaging.Address from string
         */
        public String Path
        {
            get { return qpidAddress.ToStr(); }
            set
            {
                qpidAddress = new Address(value);
            }
        }


        public bool IsTopic
        {
            get
            {
                return DestinationType == DestinationType.Topic
                    || DestinationType == DestinationType.TemporaryTopic;
            }
        }

        public bool IsQueue
        {
            get
            {
                return !IsTopic;
            }
        }


        public bool IsTemporary
        {
            get
            {
                return DestinationType == DestinationType.TemporaryQueue
                    || DestinationType == DestinationType.TemporaryTopic;
            }
        }


        /**
         * @return string representation of this instance
         */
        public override String ToString()
        {
            return Path;
        }

        
        /**
         * @return hashCode for this instance
         * TODO: figure this out
         */
        public override int GetHashCode()
        {
            int answer = 37;

            if(!String.IsNullOrEmpty(qpidAddress.Name))
            {
                answer = qpidAddress.Name.GetHashCode();
            }
            if(IsTopic)
            {
                answer ^= 0xfabfab;
            }
            return answer;
        }

        
        /**
         * if the object passed in is equivalent, return true
         *
         * @param obj the object to compare
         * @return true if this instance and obj are equivalent
         */
        public override bool Equals(Object obj)
        {
            bool result = this == obj;
            if(!result && obj != null && obj is Destination)
            {
                Destination other = (Destination) obj;
                result = this.DestinationType == other.DestinationType;
                if (!result)
                {
                    String myPath = qpidAddress.ToStr();
                    result = myPath.Equals(other.Path);
                }
            }
            return result;
        }

        /**
         * Qpid Address accessor
         * Name property
         */
        public String Name
        {
            get { return qpidAddress.Name; }
            set { qpidAddress.Name = value; }
        }

        /**
         * Qpid Address accessor
         * Subject property
         */
        public String Subject
        {
            get { return qpidAddress.Subject; }
            set { qpidAddress.Subject = value; }
        }

        /**
         * Qpid Address accessor
         * Options property
         */
        public OptionsMap Options
        {
            get { return qpidAddress.Options; }
            set { qpidAddress.Options = value; }
        }


        /**
         * Qpid Address accessor
         * Address property
         */
        public Org.Apache.Qpid.Messaging.Address Address
        {
            get { return qpidAddress; }
            set
            {
                string type = qpidAddress.Type;
                if (!type.Equals(value.Type))
                {
                    throw new NMSException("Cannot change Destination type through Address assignment");
                }
                qpidAddress = value;
            }
        }


        /**
         * Factory method to create a child destination
         * @param name
         * @return the created Destination
         */
        public abstract Destination CreateDestination(String name);


        /**
         * Factory method to create a child destination
         * @param name
         * @param subject
         * @param options variant map
         * @return the created Destination
         */
        public abstract Destination CreateDestination(String name, String subject, OptionsMap options);


        public abstract DestinationType DestinationType
        {
            get;
        }

    }
}

