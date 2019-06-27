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
    
    #region Connection Setup Attribute Class

    internal class ConnectionSetupAttribute : TestSetupAttribute
    {
        public const int DEFAULT_TEST_REQUEST_TIMEOUT = 30000;

        public string EncodingType { get; set; } = null;
        public string ClientId { get; set; } = null;

        public int MaxFrameSize { get; set; } = 0;

        public int CloseTimeout { get; set; } = 0;

        public int RequestTimeout { get; set; } = DEFAULT_TEST_REQUEST_TIMEOUT;

        protected override string InstanceName { get { return typeof(IConnection).Name; } }
        protected override string ParentName { get { return typeof(IConnectionFactory).Name; } }

        protected override int ExecuteOrder { get { return 1; } }

        public ConnectionSetupAttribute(string nmsConnectionFactoryId, params string[] nmsConnectionIds) : base(nmsConnectionFactoryId, nmsConnectionIds)
        { }
        public ConnectionSetupAttribute(string nmsConnectionFactoryId, string nmsConnectionId) : this(nmsConnectionFactoryId, new string[] { nmsConnectionId }) { }
        public ConnectionSetupAttribute(string nmsConnectionFactoryId = null) : this(nmsConnectionFactoryId, new string[] { null }) { }


        public override void BeforeTest(ITest test)
        {
            base.BeforeTest(test);
            InitializeNUnitTest<IConnection, IConnectionFactory>(test);
        }

        public override void Setup(BaseTestCase nmsTest)
        {
            base.Setup(nmsTest);
            InitializeTest<IConnection, IConnectionFactory>(nmsTest);
        }

        protected StringDictionary GetConnectionProperties(BaseTestCase nmsTest)
        {
            StringDictionary properties = new StringDictionary();
            if (EncodingType != null)
            {
                properties[NMSPropertyConstants.NMS_CONNECTION_ENCODING] = EncodingType;
            }
            if (MaxFrameSize != 0)
            {
                properties[NMSPropertyConstants.NMS_CONNECTION_MAX_FRAME_SIZE] = MaxFrameSize.ToString();
            }
            if (CloseTimeout != 0)
            {
                properties[NMSPropertyConstants.NMS_CONNECTION_CLOSE_TIMEOUT] = CloseTimeout.ToString();
            }
            if (RequestTimeout > 0)
            {
                if (properties.ContainsKey(NMSPropertyConstants.NMS_CONNECTION_REQUEST_TIMEOUT))
                {
                    properties.Add(NMSPropertyConstants.NMS_CONNECTION_REQUEST_TIMEOUT, RequestTimeout.ToString());
                }
                else
                {
                    properties[NMSPropertyConstants.NMS_CONNECTION_REQUEST_TIMEOUT] = RequestTimeout.ToString();
                }
            }//*/
            return properties;
        }

        protected IConnectionFactory GetConnectionFactory(BaseTestCase nmsTest)
        {
            IConnectionFactory cf = null;

            if (!nmsTest.NMSInstanceExists<IConnectionFactory>(parentIndex))
            {
                cf = nmsTest.CreateConnectionFactory();
                nmsTest.AddConnectionFactory(cf, NmsParentId);
            }
            else
            {
                if (NmsParentId == null)
                {
                    cf = nmsTest.GetConnectionFactory();
                }
                else
                {
                    cf = nmsTest.GetConnectionFactory(NmsParentId);
                }
            }
            BaseTestCase.Logger.Info("Found Connection Factory " + cf + "");
            return cf;
        }

        protected override T GetParentNMSInstance<T>(BaseTestCase nmsTest)
        {
            nmsTest.InitConnectedFactoryProperties(GetConnectionProperties(nmsTest));
            return (T)GetConnectionFactory(nmsTest);
        }

        protected override T CreateNMSInstance<T, P>(BaseTestCase test, P parent)
        {
            IConnection instance = test.CreateConnection((IConnectionFactory)parent);
            if (this.ClientId != null && !String.IsNullOrWhiteSpace(this.ClientId))
            {
                instance.ClientId = this.ClientId;
            }
            return (T)instance;
        }

        protected override void AddInstance<T>(BaseTestCase test, T instance, string id)
        {
            test.AddConnection((IConnection)instance, id);
        }

    }

    #endregion // end connection setup

    #region SkipTestOnRemoteBrokerSetup Attribute Class

    class SkipTestOnRemoteBrokerPropertiesAttribute : TestRestrictionSetupAttribute
    {
        #region RemoteConnectionPropertyRestriction Class

        protected class RemoteConnectionPropertyRestriction : IRestriction<IConnection>
        {
            private readonly string propertyName;
            private readonly string expectedPropertyValue;
            private string actualValue = null;

            public string PropertyName { get => this.propertyName; }
            public string PropertyValue { get => this.actualValue ?? this.expectedPropertyValue; }

            public RemoteConnectionPropertyRestriction(string propertyName, string expectedValue)
            {
                this.propertyName = propertyName;
                this.expectedPropertyValue = expectedValue;
            }

            private static bool StringCollectionContainsValueIgnoreCase(ICollection values, string key, out string foundKey)
            {
                foundKey = null;
                if (values == null || values.Count <= 0) return false;

                foreach (object o in values)
                {
                    if (o != null && o is String)
                    {
                        string value = o as string;
                        if (String.Compare(value, key, true) == 0)
                        {
                            foundKey = value;
                            return true;
                        }
                    }
                }
                return false;
            }

            public bool Apply(IConnection instance)
            {
                StringDictionary remoteConnectionProperties = 
                    ConnectionProviderUtilities.GetRemotePeerConnectionProperties(instance);
                if (remoteConnectionProperties != null)
                {
                    string restrictionKey = null;
                    if(StringCollectionContainsValueIgnoreCase(
                        remoteConnectionProperties.Keys, this.propertyName, out restrictionKey
                        ))
                    {
                        string propertyValue = remoteConnectionProperties[restrictionKey].ToString();
                        this.actualValue = propertyValue;
                        if (propertyValue != null)
                        {
                            // The restriction for this property should indicate the test is unsasfified 
                            // on a match with the expected value to skip the test on match.
                            return !propertyValue.Contains(this.expectedPropertyValue);
                        }
                    }
                }

                return true;
            }
        }

        #endregion

        #region TestSetup Properties
        protected override string InstanceName => "Remote Connection Restriction";

        protected override string ParentName => typeof(IConnection).Name;

        // must be greater then the ConnectionSetup Attribute executeOrder
        protected override int ExecuteOrder => 2;

        #endregion

        #region Skip Test Properties

        /*
         * Remote connection properties are not standard yet. 
         * The Properties described for the test attribute are taken from the 
         * Open Response frame of apache activemq 5.13.0. These properties are not 
         * guarenteed to have the same meaning across different brokers and may change 
         * across different activemq versions. For Restricting tests using specific 
         * property names refer to the broker documentation.
         * 
         */
        protected const string PLATFORM_PROPERTY_NAME = "platform";
        protected const string PRODUCT_PROPERTY_NAME = "product";
        protected const string VERSION_PROPERTY_NAME = "version";

        public string RemoteProduct { get; set; } = null;

        public string RemotePlatform { get; set; } = null;

        public string RemoteVersion { get; set; } = null;

        #endregion

        public SkipTestOnRemoteBrokerPropertiesAttribute(string parentId) : base(parentId) { }

        #region Test Setup Methods

        public override void Setup(BaseTestCase nmsTest)
        {
            base.Setup(nmsTest);
            this.RestrictTestInstance<IConnection>(nmsTest);
        }
        
        protected override P GetParentNMSInstance<P>(BaseTestCase test)
        {
            return (P)test.GetConnection(this.NmsParentId);
        }

        #endregion

        #region Test Restriction Methods

        protected override void HandleUnsatisfiedRestriction<T>(IRestriction<T> restriction, T NMSInstance)
        {
            RemoteConnectionPropertyRestriction connectionRestriction = (RemoteConnectionPropertyRestriction)restriction;
            /*
             * quietly pass test should the test restriction be unsastisfied.
             */
            Assert.Ignore(
                "Test cannot be perform on host {0} with connection property {1} = {2}", 
                TestConfig.Instance.BrokerIpAddress, 
                connectionRestriction.PropertyName, 
                connectionRestriction.PropertyValue
                );
        }

        protected override IList<IRestriction<T>> GetRestrictions<T>()
        {
            IList<IRestriction<IConnection>> set = base.GetRestrictions<IConnection>();

            /*
             * Map the Setup attribute properties to remote connection property names.
             */

            if (this.RemotePlatform != null)
            {
                set.Add(new RemoteConnectionPropertyRestriction(PLATFORM_PROPERTY_NAME, this.RemotePlatform));
            }

            if(this.RemoteProduct != null)
            {
                set.Add(new RemoteConnectionPropertyRestriction(PRODUCT_PROPERTY_NAME, this.RemoteProduct));
            }

            if(this.RemoteVersion != null)
            {
                set.Add(new RemoteConnectionPropertyRestriction(VERSION_PROPERTY_NAME, this.RemoteVersion));
            }
            return (IList<IRestriction<T>>)set;
        }

        #endregion
    }

    #endregion
    
}
