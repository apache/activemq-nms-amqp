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
using System.Collections.Specialized;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Provider;
using Apache.NMS.AMQP.Util;
using Apache.NMS.Util;
using URISupport = Apache.NMS.AMQP.Util.URISupport;

namespace Apache.NMS.AMQP
{
    public class NmsConnectionFactory : IConnectionFactory
    {
        private const string DEFAULT_REMOTE_HOST = "localhost";
        private const string DEFAULT_REMOTE_PORT = "5672";
        private Uri brokerUri;

        private IdGenerator clientIdGenerator;
        private IdGenerator connectionIdGenerator;
        private readonly object syncRoot = new object();

        public NmsConnectionFactory(string userName, string password)
        {
            UserName = userName;
            Password = password;
        }

        public NmsConnectionFactory()
        {
            BrokerUri = GetDefaultRemoteAddress();
        }

        public NmsConnectionFactory(string userName, string password, string brokerUri) : this(userName, password)
        {
            BrokerUri = CreateUri(brokerUri);
        }

        public NmsConnectionFactory(string brokerUri)
        {
            BrokerUri = CreateUri(brokerUri);
        }

        public NmsConnectionFactory(Uri brokerUri)
        {
            BrokerUri = brokerUri;
        }

        private IdGenerator ClientIdGenerator
        {
            get
            {
                if (clientIdGenerator == null)
                {
                    lock (syncRoot)
                    {
                        if (clientIdGenerator == null)
                        {
                            clientIdGenerator = ClientIdPrefix != null ? new IdGenerator(ClientIdPrefix) : new IdGenerator();
                        }
                    }
                }

                return clientIdGenerator;
            }
        }

        private IdGenerator ConnectionIdGenerator
        {
            get
            {
                if (connectionIdGenerator == null)
                {
                    lock (syncRoot)
                    {
                        if (connectionIdGenerator == null)
                        {
                            connectionIdGenerator = ConnectionIdPrefix != null ? new IdGenerator(ConnectionIdPrefix) : new IdGenerator();
                        }
                    }
                }

                return connectionIdGenerator;
            }
        }

        /// <summary>
        /// Enables local message expiry for all MessageConsumers under the connection. Default is true.
        /// </summary>
        public bool LocalMessageExpiry { get; set; } = true;

        /// <summary>
        /// User name value used to authenticate the connection
        /// </summary>
        public string UserName { get; set; }
        
        /// <summary>
        /// The password value used to authenticate the connection
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// Timeout value that controls how long the client waits on completion of various synchronous interactions, such as opening a producer or consumer,
        /// before returning an error. Does not affect synchronous message sends. By default the client will wait indefinitely for a request to complete.
        /// </summary>
        public long RequestTimeout { get; set; } = NmsConnectionInfo.DEFAULT_REQUEST_TIMEOUT;
        
        /// <summary>
        /// Timeout value that controls how long the client waits on completion of a synchronous message send before returning an error.
        /// By default the client will wait indefinitely for a send to complete.
        /// </summary>
        public long SendTimeout { get; set; } = NmsConnectionInfo.DEFAULT_SEND_TIMEOUT;

        /// <summary>
        /// Gets and sets the close timeout used to control how long a Connection close will wait for
        /// clean shutdown of the connection before giving up.  A negative value means wait
        /// forever.
        ///
        /// Care should be taken in that a very short close timeout can cause the client to
        /// not cleanly shutdown the connection and it's resources.
        ///
        /// Time in milliseconds to wait for a clean connection close.
        /// </summary>
        public long CloseTimeout { get; set; } = NmsConnectionInfo.DEFAULT_CLOSE_TIMEOUT;

        /// <summary>
        /// Optional prefix value that is used for generated Connection ID values when a new Connection is created for the NMS ConnectionFactory.
        /// This connection ID is used when logging some information from the JMS Connection object so a configurable prefix can make breadcrumbing
        /// the logs easier. The default prefix is 'ID:'.
        /// </summary>
        public string ConnectionIdPrefix { get; set; }
        
        /// <summary>
        /// Optional prefix value that is used for generated Client ID values when a new Connection is created for the JMS ConnectionFactory.
        /// The default prefix is 'ID:'.
        /// </summary>
        public string ClientIdPrefix { get; set; }

        /// <summary>
        /// Sets and gets the NMS clientId to use for connections created by this factory.
        ///
        /// NOTE: A clientID can only be used by one Connection at a time, so setting it here
        /// will restrict the ConnectionFactory to creating a single open Connection at a time.
        /// It is possible to set the clientID on the Connection itself immediately after
        /// creation if no value has been set via the factory that created it, which will
        /// allow the factory to create multiple open connections at a time.
        /// </summary>
        public string ClientId { get; set; }

        public IConnection CreateConnection()
        {
            return CreateConnection(UserName, Password);
        }

        public IConnection CreateConnection(string userName, string password)
        {
            try
            {
                NmsConnectionInfo connectionInfo = ConfigureConnectionInfo(userName, password);
                IProvider provider = ProviderFactory.Create(BrokerUri);
                return new NmsConnection(connectionInfo, provider);
            }
            catch (Exception e)
            {
                throw NMSExceptionSupport.Create(e);
            }
        }

        public INMSContext CreateContext()
        {
            return new NmsContext((NmsConnection)CreateConnection(), AcknowledgementMode.AutoAcknowledge);
        }

        public INMSContext CreateContext(AcknowledgementMode acknowledgementMode)
        {
            return new NmsContext((NmsConnection)CreateConnection(), acknowledgementMode);
        }

        public INMSContext CreateContext(string userName, string password)
        {
            return new NmsContext((NmsConnection)CreateConnection(userName, password), AcknowledgementMode.AutoAcknowledge);
        }

        public INMSContext CreateContext(string userName, string password, AcknowledgementMode acknowledgementMode)
        {
            return new NmsContext((NmsConnection)CreateConnection(userName, password), acknowledgementMode);
        }

        public Uri BrokerUri
        {
            get => brokerUri;
            set
            {
                if (value == null)
                    throw new ArgumentNullException(nameof(value), "Invalid URI: cannot be null or empty");

                brokerUri = value;

                if (URISupport.IsCompositeUri(value))
                {
                    var compositeData = NMS.Util.URISupport.ParseComposite(value);
                    SetUriOptions(compositeData.Parameters);
                    brokerUri = compositeData.toUri();
                }
                else
                {
                    StringDictionary options = NMS.Util.URISupport.ParseQuery(brokerUri.Query);
                    SetUriOptions(options);
                }
            }
        }

        public IRedeliveryPolicy RedeliveryPolicy { get; set; }
        public ConsumerTransformerDelegate ConsumerTransformer { get; set; }
        public ProducerTransformerDelegate ProducerTransformer { get; set; }

        private Uri CreateUri(string uri)
        {
            if (string.IsNullOrEmpty(uri))
            {
                throw new ArgumentException("Invalid Uri: cannot be null or empty");
            }

            try
            {
                return NMS.Util.URISupport.CreateCompatibleUri(uri);
            }
            catch (UriFormatException e)
            {
                throw new ArgumentException("Invalid Uri: " + uri, e);
            }
        }

        private NmsConnectionInfo ConfigureConnectionInfo(string userName, string password)
        {
            var connectionInfo = new NmsConnectionInfo(new NmsConnectionId(ConnectionIdGenerator.GenerateId()))
            {
                UserName = userName,
                Password = password,
                ConfiguredUri = BrokerUri,
                RequestTimeout = RequestTimeout,
                SendTimeout = SendTimeout,
                CloseTimeout = CloseTimeout,
                LocalMessageExpiry = LocalMessageExpiry
            };

            bool userSpecifiedClientId = ClientId != null;

            if (userSpecifiedClientId)
            {
                connectionInfo.SetClientId(ClientId, true);
            }
            else
            {
                connectionInfo.SetClientId(ClientIdGenerator.GenerateId(), false);
            }

            return connectionInfo;
        }

        private void SetUriOptions(StringDictionary options)
        {
            StringDictionary nmsOptions = PropertyUtil.FilterProperties(options, "nms.");
            PropertyUtil.SetProperties(this, nmsOptions);
            // TODO: Check if there are any unused options, if so throw argument exception
        }

        private Uri GetDefaultRemoteAddress()
        {
            return new Uri("amqp://" + DEFAULT_REMOTE_HOST + ":" + DEFAULT_REMOTE_PORT);
        }
    }
}