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

// Typedef for options map
using OptionsMap = System.Collections.Generic.Dictionary<System.String, System.Object>;

namespace Apache.NMS.Amqp
{

    /// <summary>
    /// Summary description for Queue.
    /// </summary>
    public class Queue : Destination, IQueue
    {

        public Queue()
            : base()
        {
        }

        public Queue(String name)
            : base(name)
        {
        }

        public Queue(String name, string subject, OptionsMap options)
            : base(name, subject, options, "queue")
        {
        }

        override public DestinationType DestinationType
        {
            get
            {
                return DestinationType.Queue;
            }
        }

        public String QueueName
        {
            get { return Path; }
        }


        public override Destination CreateDestination(String name)
        {
            return new Queue(name);
        }

        public override Destination CreateDestination(String name, string subject, OptionsMap options)
        {
            return new Queue(name, subject, options);
        }
    }
}

