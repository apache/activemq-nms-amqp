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
using System.Text;
using System.Collections.Specialized;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using Apache.NMS;
using Apache.NMS.AMQP.Test.Util;
using System.Diagnostics;
using System.Collections.Generic;
using System.Collections;
using System.Reflection;
using Apache.NMS.AMQP.Test.TestCase;

namespace Apache.NMS.AMQP.Test.Attribute
{

    #region Producer Setup Attribute Class

    internal class ProducerSetupAttribute : SessionParentDestinationDependentSetupAttribute
    {
        public static readonly long DEFAULT_TTL_LONG = Convert.ToInt64(NMSConstants.defaultTimeToLive.TotalMilliseconds);
        public MsgDeliveryMode DeliveryMode { get; set; } = NMSConstants.defaultDeliveryMode;

        public MsgPriority MsgPriority { get; set; } = NMSConstants.defaultPriority;

        public long TimeToLive { get; set; } = DEFAULT_TTL_LONG;

        public int RequestTimeout { get; set; } = System.Threading.Timeout.Infinite;

        protected override string InstanceName { get { return typeof(IMessageProducer).Name; } }

        public ProducerSetupAttribute(string parentId, string destinationId, string[] producerIds) : base(parentId, destinationId, producerIds) { }

        public ProducerSetupAttribute(string parentId, string destinationId, string producerId) : this(parentId, destinationId, new string[] { producerId }) { }

        public ProducerSetupAttribute(string parentId = null, string destinationId = null) : this(parentId, destinationId, new string[] { null }) { }

        public override void BeforeTest(ITest test)
        {
            base.BeforeTest(test);
            InitializeNUnitTest<IMessageProducer, ISession>(test);
        }

        public override void Setup(BaseTestCase nmsTest)
        {
            base.Setup(nmsTest);
            InitializeTest<IMessageProducer, ISession>(nmsTest);
        }

        protected void InitializeProducerProperties(IMessageProducer producer)
        {
            if (MsgPriority != NMSConstants.defaultPriority)
            {
                producer.Priority = MsgPriority;
            }
            if (DeliveryMode != NMSConstants.defaultDeliveryMode)
            {
                producer.DeliveryMode = DeliveryMode;
            }
            if (RequestTimeout != System.Threading.Timeout.Infinite)
            {
                producer.RequestTimeout = TimeSpan.FromMilliseconds(RequestTimeout);
            }
            if(TimeToLive != DEFAULT_TTL_LONG)
            {
                producer.TimeToLive = TimeSpan.FromMilliseconds(TimeToLive);
            }
            
        }

        protected override T CreateNMSInstance<T, P>(BaseTestCase test, P parent)
        {
            IMessageProducer producer = test.CreateProducer((ISession)parent, this.GetDestination(test));
            InitializeProducerProperties(producer);
            return (T)producer;
        }

        protected override void AddInstance<T>(BaseTestCase test, T instance, string id)
        {
            test.AddProducer((IMessageProducer)instance, id);
        }

    }

    #endregion // End Producer Setup

}
