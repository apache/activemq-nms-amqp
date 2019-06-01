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

    #region Session Setup Attribute Class

    internal class SessionSetupAttribute : TestSetupAttribute
    {
        public AcknowledgementMode AckMode { get; set; } = AcknowledgementMode.DupsOkAcknowledge;

        protected override string InstanceName
        {
            get { return typeof(ISession).Name; }
        }

        protected override string ParentName
        {
            get { return typeof(IConnection).Name; }
        }

        protected override int ExecuteOrder { get { return 2; } }

        public SessionSetupAttribute(string connectionId, params string[] sessionIds) : base(connectionId, sessionIds) { }

        public SessionSetupAttribute(string connectionId, string sessionId) : this(connectionId, new string[] { sessionId }) { }
        public SessionSetupAttribute(string connectionId = null) : this(connectionId, new string[] { null }) { }

        public override void BeforeTest(ITest test)
        {
            base.BeforeTest(test);
            InitializeNUnitTest<ISession, IConnection>(test);
        }

        public override void Setup(BaseTestCase nmsTest)
        {
            base.Setup(nmsTest);
            InitializeTest<ISession, IConnection>(nmsTest);
        }

        protected override P GetParentNMSInstance<P>(BaseTestCase test)
        {
            IConnection connection = null;
            if (!test.NMSInstanceExists<IConnection>(parentIndex))
            {
                if (NmsParentId == null)
                {
                    TestSetupFailureParentNotFound(test);
                }
                else
                {
                    connection = test.GetConnection(NmsParentId);
                }
            }
            else
            {
                connection = test.GetConnection(parentIndex);
            }
            return (P)connection;
        }

        protected override T CreateNMSInstance<T, P>(BaseTestCase test, P parent)
        {
            IConnection Parent = (IConnection)parent;
            Parent.AcknowledgementMode = AckMode;
            return (T)test.CreateSession(Parent);
        }

        protected override void AddInstance<T>(BaseTestCase test, T instance, string id)
        {
            BaseTestCase.Logger.Info("Adding Session " + id + " to test " + TestName);
            test.AddSession((ISession)instance, id);
        }

    }

    #endregion // Session Setup

}
