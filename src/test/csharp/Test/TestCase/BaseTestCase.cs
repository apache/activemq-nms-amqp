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
using Apache.NMS.AMQP.Test.Attribute;

namespace Apache.NMS.AMQP.Test.TestCase
{

    internal static class NMSTestConstants
    {
        public const string NMS_SOLACE_PLATFORM = "Solace PubSub+";
        public const string NMS_ACTIVE_PRODUCT = "ActiveMQ";
    }

    internal static class NMSPropertyConstants
    {
        public const string NMS_CONNECTION_ENCODING = "NMS.Message.Serialization";
        public const string NMS_CONNECTION_CLIENT_ID = "NMS.ClientId";
        public const string NMS_CONNECTION_USERNAME = "NMS.Username";
        public const string NMS_CONNECTION_PASSWORD = "NMS.Password";
        public const string NMS_CONNECTION_REQUEST_TIMEOUT = "NMS.RequestTimeout";
        public const string NMS_CONNECTION_MAX_FRAME_SIZE = "NMS.MaxFrameSize";
        public const string NMS_CONNECTION_CLOSE_TIMEOUT = "NMS.CloseTimeout";

        #region Transport Properties

        public const string NMS_TRANSPORT_RECEIVE_BUFFER_SIZE = "transport.ReceiveBufferSize";

        public const string NMS_TRANSPORT_RECEIVE_TIMEOUT = "transport.ReceiveTimeout";

        public const string NMS_TRANSPORT_SEND_BUFFER_SIZE = "transport.SendBufferSize";

        public const string NMS_TRANSPORT_SEND_TIMEOUT = "transport.SendTimeout";

        public const string NMS_TRANSPORT_USE_LOGGING = "transport.UseLogging";

        public const string NMS_SECURE_TRANSPORT_ACCEPT_INVALID_BROKER_CERT = "transport.AcceptInvalidBrokerCert";

        public const string NMS_SECURE_TRANSPORT_CLIENT_CERT_FILE_NAME = "transport.ClientCertFileName";

        public const string NMS_SECURE_TANSPORT_KEY_STORE_NAME = "transport.KeyStoreName";

        public const string NMS_SECURE_TANSPORT_KEY_STORE_LOCATION = "transport.KeyStoreLocation";

        public const string NMS_SECURE_TANSPORT_CLIENT_CERT_PASSWORD = "transport.ClientCertPassword";

        public const string NMS_SECURE_TANSPORT_CLIENT_CERT_SUBJECT = "transport.ClientCertSubject";

        public const string NMS_SECURE_TANSPORT_SERVER_NAME = "transport.ServerName";

        public const string NMS_SECURE_TANSPORT_SSL_PROTOCOLS = "transport.SSLProtocol";

        public const string NMS_SECURE_TRANSPORT_SSL_CHECK_CERTIFICATE_REVOCATION = "transport.CheckCertificateRevocation";

        #endregion
    }

    #region NMSTestContainer Class

    /// <summary>
    /// NMSTestContainer Root class for all tests. Container NMS object Instances for testing and manages them.
    /// </summary>
    public abstract class NMSTestContainer
    {
        public static StringDictionary Clone(StringDictionary original)
        {
            if(original == null) return null;
            StringDictionary clone = new StringDictionary();
            foreach (string key in original.Keys)
            {
                clone.Add(key.Clone() as string, original[key].Clone() as string);
            }
            return clone;
        }

        public static string ToString(IDictionary dictionary, int indt = 0)
        {
            if (dictionary == null) return "[]";
            StringBuilder sb = new StringBuilder();

            int indent = Math.Max(0, Math.Min(indt, 16));
            StringBuilder sbTabs = new StringBuilder();
            for (int i = 0; i < indent; i++)
            {
                sbTabs.Append('\t');
            }
            string wspace = sbTabs.ToString();

            sb.AppendFormat("[\n");
            foreach (object key in dictionary.Keys)
            {
                if (key != null)
                {
                    //Console.WriteLine("key: {0}, value: {1}", key, dictionary[key]);
                    
                    sb.AppendFormat("{0}\t[Key:{1}, Value: {2}]\n", wspace, key.ToString(), dictionary[key]?.ToString());
                }
            }
            sb.AppendFormat("{0}]", wspace);
            return sb.ToString();
        }

        public static string ToString(StringDictionary dictionary, int indt = 0)
        {
            if (dictionary == null) return "[]";
            StringBuilder sb = new StringBuilder();
            
            int indent = Math.Max(0, Math.Min(indt, 16));
            StringBuilder sbTabs = new StringBuilder();
            for (int i = 0; i < indent; i++)
            {
                sbTabs.Append('\t');
            }
            string wspace = sbTabs.ToString();
            
            sb.AppendFormat("{0}[\n", wspace);
            foreach (string key in dictionary.Keys)
            {
                if (key != null)
                {
                    sb.AppendFormat("{0}\t[Key:{1}, Value: {2}]\n", wspace, key, dictionary[key] ?? "null");
                }
            }
            sb.AppendFormat("{0}]", wspace);
            return sb.ToString();
        }

        protected StringDictionary properties = null;
        protected StringDictionary DefaultProperties = null;
        private Apache.NMS.NMSConnectionFactory providerFactory = null;

        private IList<IConnectionFactory> connectionFactories = new List<IConnectionFactory>();
        private IList<IConnection> connections = new List<IConnection>();
        private IList<ISession> sessions = new List<ISession>();
        private IList<IMessageProducer> producers = new List<IMessageProducer>();
        private IList<IMessageConsumer> consumers = new List<IMessageConsumer>();
        private IList<IDestination> destinations = new List<IDestination>();

        private IDictionary<string, int> connectionFactoryIndexMap = new Dictionary<string, int>();
        private IDictionary<string, int> connectionIndexMap = new Dictionary<string, int>();
        private IDictionary<string, int> sessionIndexMap = new Dictionary<string, int>();
        private IDictionary<string, int> producerIndexMap = new Dictionary<string, int>();
        private IDictionary<string, int> consumerIndexMap = new Dictionary<string, int>();
        private IDictionary<string, int> destinationIndexMap = new Dictionary<string, int>();
        private IDictionary<Type, IList> NMSInstanceTypeMap = new Dictionary<Type, IList>();
        private IDictionary<Type, IDictionary> NMSInstanceTypeIndexMap = new Dictionary<Type, IDictionary>();

        protected NMSTestContainer()
        {
            NMSInstanceTypeMap[typeof(IConnectionFactory)] = connectionFactories as List<IConnectionFactory> as IList;
            NMSInstanceTypeMap[typeof(IConnection)] = connections as List<IConnection> as IList;
            NMSInstanceTypeMap[typeof(ISession)] = sessions as List<ISession> as IList;
            NMSInstanceTypeMap[typeof(IMessageProducer)] = producers as List<IMessageProducer> as IList;
            NMSInstanceTypeMap[typeof(IMessageConsumer)] = consumers as List<IMessageConsumer> as IList;
            NMSInstanceTypeMap[typeof(IDestination)] = destinations as List<IDestination> as IList;

            NMSInstanceTypeIndexMap[typeof(IConnectionFactory)] = connectionFactoryIndexMap as Dictionary<string, int>;
            NMSInstanceTypeIndexMap[typeof(IConnection)] = connectionIndexMap as Dictionary<string, int>;
            NMSInstanceTypeIndexMap[typeof(ISession)] = sessionIndexMap as Dictionary<string, int>;
            NMSInstanceTypeIndexMap[typeof(IMessageProducer)] = producerIndexMap as Dictionary<string, int>;
            NMSInstanceTypeIndexMap[typeof(IMessageConsumer)] = consumerIndexMap as Dictionary<string, int>;
            NMSInstanceTypeIndexMap[typeof(IDestination)] = destinationIndexMap as Dictionary<string, int>;

            //try
            //{
            //    Console.WriteLine("TYPE MAP: {0}", ToString(NMSInstanceTypeMap as Dictionary<Type, IList>));
            //    Console.WriteLine("TYPE INDEX MAP: {0}", ToString(NMSInstanceTypeIndexMap as Dictionary<Type, IDictionary>));
            //}
            //catch (Exception e)
            //{
            //    Console.Error.WriteLine("Error: {0}", e.Message);
            //    Console.WriteLine(e);
            //}
        }

        public Uri BrokerURI
        {
            get { return TestConfig.Instance.BrokerUri; }
            //protected set { if (connectionFactory != null) { ConnectionFactory.BrokerUri = value; } }
        }
        internal StringDictionary ConnectionFactoryProperties
        {
            get { return Clone(properties); }
        }

        private void UpdateConnectionFactoryProperty(string key, string value)
        {
            if(properties != null && key != null)
            {
                if (this.properties.ContainsKey(key))
                {
                    this.properties[key] = value;
                }
                else 
                {
                    this.properties.Add(key, value);
                }
            }
        }

        internal void InitConnectedFactoryProperties(StringDictionary additionalProperties = null)
        {
            bool isDefault = this.properties == null;
            // add properties from the TestConfig first and use as Default Properties.
            properties = new StringDictionary();
            if (TestConfig.Instance.BrokerUsername != null)
            {
                this.UpdateConnectionFactoryProperty(NMSPropertyConstants.NMS_CONNECTION_USERNAME, TestConfig.Instance.BrokerUsername);
            }
            if (TestConfig.Instance.BrokerPassword != null)
            {
                this.UpdateConnectionFactoryProperty(NMSPropertyConstants.NMS_CONNECTION_PASSWORD, TestConfig.Instance.BrokerPassword);
            }
            if (TestConfig.Instance.ClientId != null)
            {
                ConnectionFactoryProperties[NMSPropertyConstants.NMS_CONNECTION_CLIENT_ID] = TestConfig.Instance.ClientId;
            }

            // init secure properties if broker uri is secure
            if (TestConfig.Instance.IsSecureBroker)
            {
                if (TestConfig.Instance.AcceptInvalidBrokerCert)
                {
                    string value = TestConfig.Instance.AcceptInvalidBrokerCert ? bool.TrueString : bool.FalseString;
                    this.UpdateConnectionFactoryProperty(NMSPropertyConstants.NMS_SECURE_TRANSPORT_ACCEPT_INVALID_BROKER_CERT, value);
                }
                if (TestConfig.Instance.ClientCertFileName != null)
                {
                    this.UpdateConnectionFactoryProperty(NMSPropertyConstants.NMS_SECURE_TRANSPORT_CLIENT_CERT_FILE_NAME, TestConfig.Instance.ClientCertFileName);
                }
                if (TestConfig.Instance.KeyStoreName != null)
                {
                    this.UpdateConnectionFactoryProperty(NMSPropertyConstants.NMS_SECURE_TANSPORT_KEY_STORE_NAME, TestConfig.Instance.KeyStoreName);
                }
                if (TestConfig.Instance.KeyStoreLocation != null)
                {
                    this.UpdateConnectionFactoryProperty(NMSPropertyConstants.NMS_SECURE_TANSPORT_KEY_STORE_NAME, TestConfig.Instance.KeyStoreLocation);
                }
                if (TestConfig.Instance.BrokerName != null)
                {
                    this.UpdateConnectionFactoryProperty(NMSPropertyConstants.NMS_SECURE_TANSPORT_SERVER_NAME, TestConfig.Instance.BrokerName);
                }
                if (TestConfig.Instance.ClientCertSubject != null)
                {
                    this.UpdateConnectionFactoryProperty(NMSPropertyConstants.NMS_SECURE_TANSPORT_CLIENT_CERT_SUBJECT, TestConfig.Instance.ClientCertSubject);
                }
                if (TestConfig.Instance.ClientCertPassword != null)
                {
                    this.UpdateConnectionFactoryProperty(NMSPropertyConstants.NMS_SECURE_TANSPORT_CLIENT_CERT_PASSWORD, TestConfig.Instance.ClientCertPassword);
                }
            }

            if (DefaultProperties == null || isDefault)
                DefaultProperties = Clone(properties);

            // add/overwrite Additional properies for unique purposes.
            if (additionalProperties != null)
            {
                foreach (string key in additionalProperties.Keys)
                {
                    if (properties.ContainsKey(key))
                    {
                        properties[key] = additionalProperties[key];
                    }
                    else
                    {
                        properties.Add(key, additionalProperties[key]);
                    }
                }
            }

        }

        #region NMS Instance Create Methods
        internal IConnectionFactory CreateConnectionFactory()
        {
            if (providerFactory == null)
            {
                providerFactory = new NMSConnectionFactory(BrokerURI, properties);
            }
            return providerFactory.ConnectionFactory;
        }

        internal IConnection CreateConnection(string nmsConnectionFactoryId)
        {
            return CreateConnection(GetConnectionFactory(nmsConnectionFactoryId));
        }

        internal IConnection CreateConnection(int index = 0)
        {
            return CreateConnection(GetConnectionFactory(index));
        }

        internal IConnection CreateConnection(IConnectionFactory factory)
        {
            return factory.CreateConnection();
        }

        internal ISession CreateSession(string nmsConnectionId)
        {
            return CreateSession(GetConnection(nmsConnectionId));
        }

        internal ISession CreateSession(int index = 0)
        {
            return CreateSession(GetConnection(index));
        }

        internal ISession CreateSession(IConnection connection)
        {
            return connection.CreateSession();
        }

        internal IMessageProducer CreateProducer(string nmsSessionId, string nmsDestinationId)
        {
            return CreateProducer(GetSession(nmsSessionId), GetDestination(nmsDestinationId));
        }


        internal IMessageProducer CreateProducer(int sessionIndex = 0, int destinationIndex = 0)
        {
            return CreateProducer(GetSession(sessionIndex), GetDestination(destinationIndex));
        }

        internal IMessageProducer CreateProducer(ISession session, IDestination destination)
        {
            return session.CreateProducer(destination);
        }

        internal IMessageConsumer CreateConsumer(string nmsSessionId, string nmsDestinationId)
        {
            return CreateConsumer(GetSession(nmsSessionId), GetDestination(nmsDestinationId));
        }


        internal IMessageConsumer CreateConsumer(int sessionIndex = 0, int destinationIndex = 0)
        {
            return CreateConsumer(GetSession(sessionIndex), GetDestination(destinationIndex));
        }

        internal IMessageConsumer CreateConsumer(ISession session, IDestination destination)
        {
            return session.CreateConsumer(destination);
        }

        internal ITopic CreateTopic(string name, string nmsId)
        {
            return CreateTopic(GetSession(nmsId), name);
        }

        internal ITopic CreateTopic(string name, int index = 0)
        {
            return CreateTopic(GetSession(index), name);
        }

        internal ITopic CreateTopic(ISession session, string name)
        {
            return session.GetTopic(name);
        }

        internal ITopic CreateTemporaryTopic(string nmsId)
        {
            return CreateTemporaryTopic(GetSession(nmsId));
        }

        internal ITopic CreateTemporaryTopic(int index = 0)
        {
            return CreateTemporaryTopic(GetSession(index));
        }

        internal ITemporaryTopic CreateTemporaryTopic(ISession session)
        {
            return session.CreateTemporaryTopic();
        }

        internal IQueue CreateQueue(string name, string nmsId)
        {
            return CreateQueue(GetSession(nmsId), name);
        }

        internal IQueue CreateQueue(string name, int index = 0)
        {
            return CreateQueue(GetSession(index), name);
        }

        internal IQueue CreateQueue(ISession session, string name)
        {
            return session.GetQueue(name);
        }

        internal IQueue CreateTemporaryQueue(string nmsId)
        {
            return CreateTemporaryQueue(GetSession(nmsId));
        }

        internal IQueue CreateTemporaryQueue(int index = 0)
        {
            return CreateTemporaryQueue(GetSession(index));
        }

        internal ITemporaryQueue CreateTemporaryQueue(ISession session)
        {
            return session.CreateTemporaryQueue();
        }


        #endregion

        #region NMS Instance Get Methods

        internal IConnectionFactory GetConnectionFactory(int index = 0)
        {
            return LookupNMSInstance(connectionFactories, index);
        }

        internal IConnectionFactory GetConnectionFactory(string nmsId)
        {
            int index = connectionFactoryIndexMap[nmsId];
            return LookupNMSInstance(connectionFactories, index);
        }

        internal IConnection GetConnection(int index = 0)
        {
            return LookupNMSInstance(connections, index);
        }

        internal IConnection GetConnection(string nmsId)
        {
            int index = connectionIndexMap[nmsId];
            return LookupNMSInstance(connections, index);
        }

        internal ISession GetSession(int index = 0)
        {
            return LookupNMSInstance(sessions, index);
        }

        internal ISession GetSession(string nmsId)
        {
            //Console.WriteLine("Trying to find Session {0}, in Index Map {1}.", nmsId, ToString(sessionIndexMap as Dictionary<String, int>));
            int index = sessionIndexMap[nmsId];
            return LookupNMSInstance(sessions, index);
        }

        internal IMessageProducer GetProducer(int index = 0)
        {
            return LookupNMSInstance(producers, index);
        }

        internal IMessageProducer GetProducer(string nmsId)
        {
            int index = producerIndexMap[nmsId];
            return LookupNMSInstance(producers, index);
        }

        internal IMessageConsumer GetConsumer(int index = 0)
        {
            return LookupNMSInstance(consumers, index);
        }
        internal IMessageConsumer GetConsumer(string nmsId)
        {
            int index = consumerIndexMap[nmsId];
            return LookupNMSInstance(consumers, index);
        }

        internal IDestination GetDestination(int index = 0)
        {
            return LookupNMSInstance(destinations, index);
        }

        internal IDestination GetDestination(string nmsId)
        {
            int index = destinationIndexMap[nmsId];
            return LookupNMSInstance(destinations, index);
        }

        internal IList<IConnection> GetConnections(params string[] nmsIds)
        {
            return GetNMSIntances<IConnection>(nmsIds);
        }

        internal IList<ISession> GetSessions(params string[] nmsIds)
        {
            return GetNMSIntances<ISession>(nmsIds);
        }
        internal IList<IDestination> GetDestinations(params string[] nmsIds)
        {
            return GetNMSIntances<IDestination>(nmsIds);
        }
        internal IList<IMessageConsumer> GetConsumers(params string[] nmsIds)
        {
            return GetNMSIntances<IMessageConsumer>(nmsIds);
        }
        internal IList<IMessageProducer> GetProducers(params string[] nmsIds)
        {
            return GetNMSIntances<IMessageProducer>(nmsIds);
        }

        private IList<I> GetNMSIntances<I>(params string[] nmsIds)
        {
            IDictionary NMSInstanceIndexMap = NMSInstanceTypeIndexMap[typeof(I)];
            IList NMSInstances = NMSInstanceTypeMap[typeof(I)];
            return GetNMSIntances<I>(NMSInstanceIndexMap as Dictionary<string,int>, NMSInstances as List<I>, nmsIds);
        }

        private IList<I> GetNMSIntances<I>(IDictionary<string, int> indexMap, IList<I> instances, params string[] nmsIds)
        {
            if (nmsIds == null)
            {
                return null;
            }
            else if (nmsIds.Length == 0)
            {
                return new List<I>();
            }
            List<I> list = new List<I>();
            foreach (string id in nmsIds)
            {
                int index = -1;
                if (indexMap.TryGetValue(id, out index))
                {
                    try
                    {
                        list.Add(LookupNMSInstance(instances, index));
                    }
                    catch (Exception ex)
                    {
                        BaseTestCase.Logger.Error("Caught exception when looking up NMS Instance " + typeof(I).Name + " for Id \"" + id + "\" Message : " + ex.Message );
                    }
                }
            }

            return list;
        }

        private I LookupNMSInstance<I>(IList<I> list, int index = 0)
        {
            if (index < 0 || index >= list.Count)
            {
                throw new ArgumentOutOfRangeException(string.Format("Invalid index {0}, for {1} Collection.", index, typeof(I).Name));
            }
            
            I nmsInstance = list[index];
            if (nmsInstance == null)
            {
                throw new ArgumentException(string.Format("Invalid index {0}, for {1} Collection. Return value is null.", index, typeof(I).Name));
            }
            return nmsInstance;
        }

        #endregion
        
        #region NMS Instance Exists Methods
        
        internal bool NMSInstanceExists<I>(int index)
        {
            IList NMSInstances = NMSInstanceTypeMap[typeof(I)];
            if (index < 0 || index >= NMSInstances.Count) return false;
            return NMSInstances[index] != null;
        }

        internal bool NMSInstanceExists<I>(string nmsId)
        {
            IDictionary NMSInstances = NMSInstanceTypeIndexMap[typeof(I)];
            if (NMSInstances.Contains(nmsId)) return false;
            int index = (int)NMSInstances[nmsId];
            return NMSInstanceExists<I>(index);
        }


        #endregion

        #region NMS Instance Add Methods

        internal void AddConnectionFactory(IConnectionFactory factory, string nmsId=null)
        {
            int index = AddNMSInstance(connectionFactories, factory);
            AddNMSInstanceIndexLookup(connectionFactoryIndexMap, nmsId, index);
        }

        internal void AddConnection(IConnection connection, string nmsId = null)
        {
            int index = AddNMSInstance(connections, connection);
            AddNMSInstanceIndexLookup(connectionIndexMap, nmsId, index);
        }

        internal void AddSession(ISession session, string nmsId = null)
        {
            int index = AddNMSInstance(sessions, session);
            AddNMSInstanceIndexLookup(sessionIndexMap, nmsId, index);
        }

        internal void AddProducer(IMessageProducer producer, string nmsId = null)
        {
            int index = AddNMSInstance(producers, producer);
            AddNMSInstanceIndexLookup(producerIndexMap, nmsId, index);
        }

        internal void AddConsumer(IMessageConsumer consumer, string nmsId = null)
        {
            int index = AddNMSInstance(consumers, consumer);
            AddNMSInstanceIndexLookup(consumerIndexMap, nmsId, index);
        }

        internal void AddDestination(IDestination destination, string nmsId = null)
        {
            int index = AddNMSInstance(destinations, destination);
            AddNMSInstanceIndexLookup(destinationIndexMap, nmsId, index);
        }


        private int AddNMSInstance<I>(IList<I> list, I instance)
        {
            if (instance == null)
            {
                return -1;
            }
            int previousIndex = list.IndexOf(instance);
            if (list.Contains(instance)) return previousIndex;
            list.Add(instance);
            return list.IndexOf(instance);
        }

        private void AddNMSInstanceIndexLookup(IDictionary<string,int>lookupTable, string nmsId, int index)
        {
            if (index != -1 && nmsId != null)
            {
                lookupTable.Add(nmsId, index);
            }
        }

        #endregion

        #region Object Override Methods

        public override string ToString()
        {
            string result = "" + this.GetType().Name + ": [\n";
            result += "\tConnection Factories: " + this.connectionFactories.Count + "\n";
            result += "\tConnections: " + this.connections.Count + "\n";
            result += "\tSessions: " + this.sessions.Count + "\n";
            result += "\tProducers: " + this.producers.Count + "\n";
            result += "\tConsumers: " + this.consumers.Count + "\n";
            result += "\tDestinations: " + this.destinations.Count + "\n";
            result += "\tConnection Factory Index Table: " + ToString(this.connectionFactoryIndexMap as IDictionary, 1) + "\n";
            result += "\tConnection Index Table: " + ToString(this.connectionIndexMap as IDictionary, 1) + "\n";
            result += "\tSession Index Table: " + ToString(this.sessionIndexMap as IDictionary, 1) + "\n";
            result += "\tConsumer Index Table: " + ToString(this.consumerIndexMap as IDictionary, 1) + "\n";
            result += "\tProducer Index Table: " + ToString(this.producerIndexMap as IDictionary, 1) + "\n";
            result += "\tDestination Index Table: " + ToString(this.destinationIndexMap as IDictionary, 1) + "\n";
            result += "]";
            return result;
        }

        #endregion

        #region NMS Instance Management Cleanup

        protected void CleanupInstances(bool dispose = false)
        {
            CloseInstances();
            ClearIndexes();
            ClearInstances(dispose);
            GC.Collect();
        }

        private void CloseInstances()
        {
            if (this.producers != null)
            {
                foreach(IMessageProducer p in producers)
                {
                    p?.Close();
                }
            }

            if (this.consumers != null)
            {
                foreach(IMessageConsumer c in consumers)
                {
                    c?.Close();
                }
            }

            if (this.sessions != null)
            {
                foreach(ISession s in sessions)
                {
                    s?.Close();
                }
            }

            if (this.connections != null)
            {
                foreach(IConnection c in connections)
                {
                    c?.Close();
                }
            }
            
        }

        private void ClearIndexes()
        {
            foreach(IDictionary indexTable in NMSInstanceTypeIndexMap.Values)
            {
                indexTable.Clear();
            }
        }

        private void ClearInstances(bool dispose = false)
        {
            
            if (dispose && this.producers != null)
            {
                foreach (IMessageProducer p in producers)
                {
                    p?.Dispose();
                }
            }
            producers?.Clear();

            if (dispose && this.consumers != null)
            {
                foreach (IMessageConsumer c in consumers)
                {
                    c?.Dispose();
                }
            }
            consumers?.Clear();

            if (dispose && this.sessions != null)
            {
                foreach (ISession s in sessions)
                {
                    s?.Close();
                }
            }
            sessions?.Clear();

            if (dispose && this.connections != null)
            {
                foreach (IConnection c in connections)
                {
                    c?.Close();
                }
            }
            connections?.Clear();

            connectionFactories?.Clear();

            providerFactory = null;
        }

        #endregion

    }

    #endregion

    #region BaseTestCase Class

    public abstract class BaseTestCase : NMSTestContainer
    {
        
        internal static ITrace Logger = new NMSLogger(NMSLogger.ToLogLevel(TestConfig.Instance.LogLevel));

        internal const string DURABLE_TOPIC_NAME = "nms.durable.test";
        internal const string DURABLE_SUBSRIPTION_NAME = "uniqueSub";

        static BaseTestCase()
        {
            Tracer.Trace = Logger;
        }

        public virtual bool IsSecureBroker { get => TestConfig.Instance.IsSecureBroker; }

        private static readonly TestSetupAttributeComparer TestSetupOrderComparer = new TestSetupAttributeComparer();
        private class TestSetupAttributeComparer : IComparer<TestSetupAttribute>
        {
            public int Compare(TestSetupAttribute x, TestSetupAttribute y)
            {
                int result = y.ComparableOrder - x.ComparableOrder;
                if(result == 0)
                {
                    result = x.CompareTo(y);
                }
                return result;
            }
        }
        
        private void ApplyTestSetupAttributes()
        {
            // Apply TestSetup Attribute in correct order
            TestContext.TestAdapter testAdapter = TestContext.CurrentContext.Test;
            MethodInfo methodInfo = GetType().GetMethod(testAdapter.MethodName);
            object[] attributes = methodInfo.GetCustomAttributes(true);

            // This set will order the TestSetup Attributes in the appropriate execution order for NMS Instance Initialization.
            // ie, should a test setup a connection and a session dependent that connection it ensure the connection setup attribute
            // execute its setup first.
            ISet<TestSetupAttribute> testSetupAttributes = new SortedSet<TestSetupAttribute>(TestSetupOrderComparer);
            foreach (System.Attribute attribute in attributes)
            {
                if (attribute is TestSetupAttribute)
                {
                    //Console.WriteLine("Setup Attribute Identification: {0}.", attribute.GetType().Name);
                    testSetupAttributes.Add(attribute as TestSetupAttribute);
                }
            }
            foreach (TestSetupAttribute tsa in testSetupAttributes)
            {
                //Console.WriteLine("Setup Attribute: {0}.", tsa.GetType().Name);
                try
                {
                    tsa.Setup(this);
                }
                catch (Exception ex)
                {
                    this.PrintTestFailureAndAssert(testAdapter.MethodName, "Failed to setup test attribute.", ex);
                }

            }
        }

        [SetUp]
        public virtual void Setup()
        {
            Logger.Info(string.Format("Setup TestCase {0} for test {1}.",
                this.GetType().Name, TestContext.CurrentContext.Test.MethodName));
            try
            {
                // Setup NMS Instances for test.
                ApplyTestSetupAttributes();
            }
            catch(Exception ex)
            {
                this.PrintTestFailureAndAssert(GetTestMethodName(), 
                    "Failure in setup attributes.", ex);
            }
            finally
            {
                // Always Setup common test varibles.
                msgCount = 0;
                asyncEx = null;
                StopOnAsyncFailure = true;
            }
            
        }

        [TearDown]
        public virtual void TearDown()
        {
            Logger.Info(string.Format("Tear Down TestCase {0} for test {1}.", this.GetType().Name, TestContext.CurrentContext.Test.MethodName));
            waiter?.Dispose();
            // restore properties for next test
            properties = Clone(DefaultProperties);
            CleanupInstances();
            Logger.Info(string.Format("Tear Down Finished for TestCase {0} for test {1}.", this.GetType().Name, TestContext.CurrentContext.Test.MethodName));
        }

        protected string GetMethodName()
        {
            StackTrace stackTrace = new StackTrace();
            return stackTrace.GetFrame(1).GetMethod().Name;
        }



        protected string GetTestMethodName()
        {
            TestContext.TestAdapter testAdapter = TestContext.CurrentContext.Test;
            return testAdapter.MethodName;
        }

        internal virtual void PrintTestFailureAndAssert(string methodDescription, string info, Exception ex)
        {
            if (ex is AssertionException || ex is IgnoreException || ex is SuccessException)
            {
                throw ex;
            }
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("@@Test Failed in {0}", methodDescription);
            if (info != null)
            {
                sb.AppendFormat(", where info = {0}", info);
            }
            if (ex != null)
            {
                sb.AppendFormat(", {0}\n", GetTestException(ex));
            }
            
            Logger.Error(sb.ToString());
            Assert.Fail(sb.ToString());
        }

        protected virtual string GetTestException(Exception ex)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("Encountered an exception:\n\tMessage = {0}\n", ex.Message);
            sb.AppendFormat("\tType = {0}\n", ex.GetType());
            sb.AppendFormat("\tStack = {0}\n", ex.StackTrace);
            if(ex is NMSException)
            {
                string errcode = (ex as NMSException).ErrorCode;
                if (errcode != null && errcode.Length > 0)
                {
                    sb.AppendFormat("\tErrorCode = {0}\n", errcode);
                }
            }
            if (null != ex.InnerException )
            {
                sb.AppendFormat("Inner Exception:\n\t{0}", GetTestException(ex.InnerException));
            }
            return sb.ToString();
            
        }

        internal void PrintTestException(Exception ex)
        {
            
            Logger.Error(GetTestException(ex));
        }

        #region Common Test properties and Components

        protected int msgCount = 0;
        protected Exception asyncEx = null;
        protected System.Threading.ManualResetEvent waiter;
        protected bool StopOnAsyncFailure = true;

        protected virtual void DefaultExceptionListener(Exception ex)
        {
            this.PrintTestException(ex);
            asyncEx = ex;
            if(waiter!= null && StopOnAsyncFailure)
                waiter.Set();
        }

        protected virtual MessageListener CreateListener(int expectedMsgs)
        {
            return (m) =>
            {
                DefaultMessageListener(m);
                if (msgCount >= expectedMsgs)
                {
                    if(waiter != null)
                    {
                        Logger.Debug(string.Format("Received all msgs ({0}) on callback.", msgCount));
                        waiter.Set();
                    }
                        
                }
            };
        }

        protected long ExtractMsgId(string nmsMsgId)
        {
            long result = -1;
            int index = nmsMsgId.LastIndexOf(':');
            if (index >= 0)
            {
                try
                {
                    result = Convert.ToInt64(nmsMsgId.Substring(index + 1));
                }
                catch (Exception e)
                {
                    Logger.Warn("Failed to extract Msg Id from nmsMsgId " + nmsMsgId + " Cause: " + e.Message);
                }
            }
            return result;
        }

        protected virtual void DefaultMessageListener(IMessage message)
        {
            msgCount++;
            Logger.Debug(string.Format("Received msg {0} on Async Callback.(Count = {1})", message.NMSMessageId, msgCount));
        }


        #endregion

    }

    #endregion

}
