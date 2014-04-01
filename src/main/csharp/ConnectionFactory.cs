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
using System.Collections;
using System.Collections.Specialized;
using Apache.NMS.Policies;
using Org.Apache.Qpid.Messaging;

namespace Apache.NMS.Amqp
{
    /// <summary>
    /// A Factory that can establish NMS connections to AMQP using QPID.
    /// 
    /// @param brokerUri String or Uri specifying base connection address
    /// @param clientID String specifying client ID
    /// @param connectionProperties formed by one of:
    ///     * 0..N Strings specifying Qpid connection connectionProperties in the form "name:value".
    ///     * Hashtable containing properties as key/value pairs
    /// 
    /// Connection URI are defined in
    /// http://qpid.apache.org/releases/qpid-trunk/programming/book/connections.html#connection-url
    /// 
    /// Example using property strings:
    /// 
    /// Uri connecturi = new Uri("amqp:localhost:5673")
    /// IConnectionFactory factory = new NMSConnectionFactory();
    /// IConnectionFactory factory = new NMSConnectionFactory(connecturi);
    /// IConnectionFactory factory = new NMSConnectionFactory(connecturi, "UserA");
    /// IConnectionFactory factory = new NMSConnectionFactory(connecturi, "UserA", "protocol:amqp1.0");
    /// IConnectionFactory factory = new NMSConnectionFactory(connecturi, "UserA", "protocol:amqp1.0", "reconnect:true", "reconnect_timeout:60", "username:bob", "password:secret");
    /// 
    /// Example using property table:
    /// 
    /// Uri connecturi = new Uri("amqp:localhost:5672")
    /// Hashtable properties = new Hashtable();
    /// properties.Add("protocol", "amqp1.0");
    /// properties.Add("reconnect_timeout", 60)
    /// IConnectionFactory factory = new NMSConnectionFactory(connecturi, "UserA", properties);
    /// 
    /// See http://qpid.apache.org/components/programming/book/connection-options.html 
    /// for more information on Qpid connection options.
    /// </summary>
    public class ConnectionFactory : IConnectionFactory
    {
        public const string DEFAULT_BROKER_URL = "tcp://localhost:5672";
        public const string ENV_BROKER_URL = "AMQP_BROKER_URL";
        private const char SEP_NAME_VALUE = ':';

        private Uri brokerUri;
        private string clientID;

        private StringDictionary properties = new StringDictionary();
        private IRedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();

        #region Constructor Methods

        public static string GetDefaultBrokerUrl()
        {
            string answer = Environment.GetEnvironmentVariable(ENV_BROKER_URL);
            if (answer == null)
            {
                answer = DEFAULT_BROKER_URL;
            }
            return answer;
        }

        public ConnectionFactory()
            : this(new Uri(GetDefaultBrokerUrl()), string.Empty, (Object[])null)
        {
        }

        public ConnectionFactory(string brokerUri)
            : this(new Uri(brokerUri), string.Empty, (Object[])null)
        {
        }

        public ConnectionFactory(string brokerUri, string clientID)
            : this(new Uri(brokerUri), clientID, (Object[])null)
        {
        }

        public ConnectionFactory(Uri brokerUri)
            : this(brokerUri, string.Empty, (Object[])null)
        {
        }

        public ConnectionFactory(Uri brokerUri, string clientID)
            : this(brokerUri, clientID, (Object[])null)
        {
        }

        public ConnectionFactory(Uri brokerUri, string clientID, params Object[] propsArray)
        {
            Tracer.DebugFormat("Amqp: create connection factory for Uri: {0}", brokerUri.ToString()); 
            try
            {
                this.brokerUri = brokerUri;
                this.clientID = clientID;

                if (propsArray != null)
                {
                    foreach (object prop in propsArray)
                    {
                        string nvp = prop.ToString();
                        int sepPos = nvp.IndexOf(SEP_NAME_VALUE);
                        if (sepPos > 0)
                        {
                            properties.Add(nvp.Substring(0, sepPos), nvp.Substring(sepPos + 1));
                        }
                        else
                        {
                            throw new NMSException("Connection property is not in the form \"name:value\" :" + nvp);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Apache.NMS.Tracer.DebugFormat("Exception instantiating AMQP.ConnectionFactory: {0}", ex.Message);
                throw;
            }
        }

        public ConnectionFactory(Uri brokerUri, string clientID, Hashtable propsTable)
        {
            Tracer.DebugFormat("Amqp: create connection factory for Uri: {0}", brokerUri.ToString());
            try
            {
                this.brokerUri = brokerUri;
                this.clientID = clientID;

                if (properties != null)
                {
                    foreach (var key in propsTable.Keys)
                    {
                        properties.Add(key.ToString(), propsTable[key].ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                Apache.NMS.Tracer.DebugFormat("Exception instantiating AMQP.ConnectionFactory: {0}", ex.Message);
                throw;
            }
        }

        #endregion

        #region IConnectionFactory Members

        /// <summary>
        /// Creates a new connection to Qpid/Amqp.
        /// </summary>
        public IConnection CreateConnection()
        {
            return CreateConnection(string.Empty, string.Empty);
        }

        /// <summary>
        /// Creates a new connection to Qpid/Amqp.
        /// </summary>
        public IConnection CreateConnection(string userName, string password)
        {
            Connection connection = new Connection();

            connection.RedeliveryPolicy = this.redeliveryPolicy.Clone() as IRedeliveryPolicy;
            //connection.ConsumerTransformer = this.consumerTransformer; // TODO:
            //connection.ProducerTransformer = this.producerTransformer; // TODO:
            connection.BrokerUri = this.BrokerUri;
            connection.ClientId = this.clientID;
            connection.ConnectionProperties = this.properties;

            if (!String.IsNullOrEmpty(userName))
            {
                connection.SetConnectionProperty(Connection.USERNAME_OPTION, userName);
            }
            if (!String.IsNullOrEmpty(password))
            {
                connection.SetConnectionProperty(Connection.PASSWORD_OPTION, password);
            }

            IConnection ReturnValue = null;
            ReturnValue = connection;

            return ReturnValue;
        }

        /// <summary>
        /// Get/or set the broker Uri.
        /// </summary>
        public Uri BrokerUri
        {
            get { return brokerUri; }
            set { brokerUri = value; }
        }

        /// <summary>
        /// Get/or set the redelivery policy that new IConnection objects are
        /// assigned upon creation.
        /// </summary>
        public IRedeliveryPolicy RedeliveryPolicy
        {
            get { return this.redeliveryPolicy; }
            set
            {
                if (value != null)
                {
                    this.redeliveryPolicy = value;
                }
            }
        }

        private ConsumerTransformerDelegate consumerTransformer;
        public ConsumerTransformerDelegate ConsumerTransformer
        {
            get { return this.consumerTransformer; }
            set { this.consumerTransformer = value; }
        }

        private ProducerTransformerDelegate producerTransformer;
        public ProducerTransformerDelegate ProducerTransformer
        {
            get { return this.producerTransformer; }
            set { this.producerTransformer = value; }
        }

        #endregion

        #region ConnectionProperties Methods

        /// <summary>
        /// Connection connectionProperties acceessor
        /// </summary>
        /// <remarks>This factory does not check for legal property names. Users
        /// my specify anything they want. Propery name processing happens when
        /// connections are created and started.</remarks>
        public StringDictionary ConnectionProperties
        {
            get { return properties; }
            set { properties = value; }
        }

        /// <summary>
        /// Test existence of named property
        /// </summary>
        /// <param name="name">The name of the connection property to test.</param>
        /// <returns>Boolean indicating if property exists in setting dictionary.</returns>
        public bool ConnectionPropertyExists(string name)
        {
            return properties.ContainsKey(name);
        }

        /// <summary>
        /// Get value of named property
        /// </summary>
        /// <param name="name">The name of the connection property to get.</param>
        /// <returns>string value of property.</returns>
        /// <remarks>Throws if requested property does not exist.</remarks>
        public string GetConnectionProperty(string name)
        {
            if (properties.ContainsKey(name))
            {
                return properties[name];
            }
            else
            {
                throw new NMSException("Amqp connection property '" + name + "' does not exist");
            }
        }

        /// <summary>
        /// Set value of named property
        /// </summary>
        /// <param name="name">The name of the connection property to set.</param>
        /// <param name="value">The value of the connection property.</param>
        /// <returns>void</returns>
        /// <remarks>Existing property values are overwritten. New property values
        /// are added.</remarks>
        public void SetConnectionProperty(string name, string value)
        {
            if (properties.ContainsKey(name))
            {
                properties[name] = value;
            }
            else
            {
                properties.Add(name, value);
            }
        }
        #endregion
    }
}
