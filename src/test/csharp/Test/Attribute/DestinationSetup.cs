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

    #region Destination Setup Attribute Class

    abstract class DestinationSetupAttribute : SessionParentSetupAttribute
    {
        public string Name { get; set; }

        protected DestinationSetupAttribute(string pId, string[] destinationIds) : base(pId, destinationIds) { }

        protected override void AddInstance<T>(BaseTestCase test, T instance, string id)
        {
            test.AddDestination((IDestination)instance, id);
        }
    }

    #endregion // end Destination Attribute Class

    #region Topic, Queue, Temp Topic, Temp Queue Atribute Classes

    internal class TopicSetupAttribute : DestinationSetupAttribute
    {
        protected override string InstanceName { get { return typeof(ITopic).Name; } }

        public TopicSetupAttribute(string pId, params string[] destinationIds) : base(pId, destinationIds) { }

        public TopicSetupAttribute(string pId, string destinationId) : this(pId, new string[] { destinationId }) { }

        public TopicSetupAttribute(string pId = null) : this(pId, new string[] { null }) { }

        public override void BeforeTest(ITest test)
        {
            base.BeforeTest(test);
            InitializeNUnitTest<ITopic, ISession>(test);
        }

        public override void Setup(BaseTestCase nmsTest)
        {
            base.Setup(nmsTest);
            InitializeTest<ITopic, ISession>(nmsTest);
        }

        protected override T CreateNMSInstance<T, P>(BaseTestCase test, P parent)
        {
            ISession session = (ISession)parent;
            return (T)test.CreateTopic(session, Name);
        }
    }

    internal class TemporaryTopicSetupAttribute : DestinationSetupAttribute
    {
        protected override string InstanceName { get { return typeof(ITemporaryTopic).Name; } }

        public TemporaryTopicSetupAttribute(string pId, string[] destinationIds) : base(pId, destinationIds) { }

        public TemporaryTopicSetupAttribute(string pId, string destinationId) : this(pId, new string[] { destinationId }) { }

        public TemporaryTopicSetupAttribute(string pId = null) : this(pId, new string[] { null }) { }

        public override void BeforeTest(ITest test)
        {
            base.BeforeTest(test);
            InitializeNUnitTest<ITemporaryTopic, ISession>(test);
        }

        public override void Setup(BaseTestCase nmsTest)
        {
            base.Setup(nmsTest);
            InitializeTest<ITemporaryTopic, ISession>(nmsTest);
        }

        protected override T CreateNMSInstance<T, P>(BaseTestCase test, P parent)
        {
            ISession session = (ISession)parent;
            return (T)test.CreateTemporaryTopic(session);
        }
    }

    internal class QueueSetupAttribute : DestinationSetupAttribute
    {
        protected override string InstanceName { get { return typeof(IQueue).Name; } }

        public QueueSetupAttribute(string pId, params string[] destinationIds) : base(pId, destinationIds) { }

        public QueueSetupAttribute(string pId, string destinationId) : this(pId, new string[] { destinationId }) { }

        public QueueSetupAttribute(string pId = null) : this(pId, new string[] { null }) { }

        public override void BeforeTest(ITest test)
        {
            base.BeforeTest(test);
            InitializeNUnitTest<IQueue, ISession>(test);
        }

        public override void Setup(BaseTestCase nmsTest)
        {
            base.Setup(nmsTest);
            InitializeTest<IQueue, ISession>(nmsTest);
        }

        protected override T CreateNMSInstance<T, P>(BaseTestCase test, P parent)
        {
            ISession session = (ISession)parent;
            return (T)test.CreateQueue(session, Name);
        }
    }

    internal class TemporaryQueueSetupAttribute : DestinationSetupAttribute
    {
        protected override string InstanceName { get { return typeof(ITemporaryQueue).Name; } }

        public TemporaryQueueSetupAttribute(string pId, string[] destinationIds) : base(pId, destinationIds) { }

        public TemporaryQueueSetupAttribute(string pId, string destinationId) : this(pId, new string[] { destinationId }) { }

        public TemporaryQueueSetupAttribute(string pId = null) : this(pId, new string[] { null }) { }

        public override void BeforeTest(ITest test)
        {
            base.BeforeTest(test);
            InitializeNUnitTest<ITemporaryQueue, ISession>(test);
        }

        public override void Setup(BaseTestCase nmsTest)
        {
            base.Setup(nmsTest);
            InitializeTest<ITemporaryQueue, ISession>(nmsTest);
        }

        protected override T CreateNMSInstance<T, P>(BaseTestCase test, P parent)
        {
            ISession session = (ISession)parent;
            return (T)test.CreateTemporaryQueue(session);
        }
    }

    #endregion // End Topic, Queue, Temp Topic, Temp Queue Attribute Classes



}
