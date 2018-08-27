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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.NMS;
using Apache.NMS.Util;
using Apache.NMS.Policies;
using Apache.NMS.AMQP.Util;
using Apache.NMS.AMQP.Transport;
using Apache.NMS.AMQP.Transport.AMQP;
using Apache.NMS.AMQP.Transport.Secure;
using Apache.NMS.AMQP.Transport.Secure.AMQP;
using System.Security.Cryptography.X509Certificates;
using System.Net.Security;
using System.Security.Authentication;
using Amqp;

namespace Apache.NMS.AMQP
{
    internal delegate Task<Amqp.Connection> ProviderCreateConnection(Amqp.Address addr, Amqp.Framing.Open open, Amqp.OnOpened onOpened);
    /// <summary>
    /// Apache.NMS.AMQP.ConnectionFactory implements Apache.NMS.IConnectionFactory.
    /// Apache.NMS.AMQP.ConnectionFactory creates, manages and configures the Amqp.ConnectionFactory used to create Amqp Connections.
    /// </summary>
    public class ConnectionFactory : Apache.NMS.IConnectionFactory
    {

        public const string DEFAULT_BROKER_URL = "tcp://localhost:5672";
        internal static readonly string CLIENT_ID_PROP = PropertyUtil.CreateProperty("ClientId", "", ConnectionPropertyPrefix);
        internal static readonly string USERNAME_PROP = PropertyUtil.CreateProperty("UserName", "", ConnectionPropertyPrefix);
        internal static readonly string PASSWORD_PROP = PropertyUtil.CreateProperty("Password", "", ConnectionPropertyPrefix);

        internal const string ConnectionPropertyPrefix = "connection.";
        internal const string ConnectionPropertyAlternativePrefix = PropertyUtil.PROPERTY_PREFIX;
        internal const string TransportPropertyPrefix = "transport.";

        private Amqp.Address amqpHost = null;
        private Uri brokerUri;
        private string clientId;
        private IdGenerator clientIdGenerator = new IdGenerator();
        
        private StringDictionary properties = new StringDictionary();
        private StringDictionary applicationProperties = null;

        private TransportPropertyInterceptor transportProperties;
        private ConnectionFactoryPropertyInterceptor connectionProperties;
        private IRedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
        
        private Amqp.ConnectionFactory impl;
        private TransportContext transportContext;

        #region Constructor Methods

        public ConnectionFactory()
            : this(DEFAULT_BROKER_URL)
        {
        }

        public ConnectionFactory(string brokerUri)
            : this(URISupport.CreateCompatibleUri(brokerUri), null, null)
        {

        }

        public ConnectionFactory(string brokerUri, string clientId)
            : this(URISupport.CreateCompatibleUri(brokerUri), clientId, null)
        {
        }

        public ConnectionFactory(Uri brokerUri)
            : this(brokerUri, null, null)
        { }

        public ConnectionFactory(Uri brokerUri, StringDictionary props)
            : this(brokerUri, null, props)
        { }

        public ConnectionFactory(Uri brokerUri, string clientId, StringDictionary props)
        {
            impl = new Amqp.ConnectionFactory();
            this.clientId = clientId;
            if (props != null)
            {
                this.InitApplicationProperties(props);
            }
            BrokerUri = brokerUri;
            impl.AMQP.HostName = BrokerUri.Host;
            //
            // Set up tracing in AMQP.  We capture all AMQP traces in the TraceListener below
            // and map to NMS 'Tracer' logs as follows:
            //    AMQP          Tracer
            //    Verbose       Debug
            //    Frame         Debug
            //    Information   Info
            //    Output        Info    (should not happen)
            //    Warning       Warn
            //    Error         Error
            //
            Amqp.Trace.TraceLevel = Amqp.TraceLevel.Verbose | Amqp.TraceLevel.Frame;
            Amqp.Trace.TraceListener = (level, format, args) =>
            {
                switch (level)
                {
                    case Amqp.TraceLevel.Verbose:
                    case Amqp.TraceLevel.Frame:
                        Tracer.DebugFormat(format, args);
                        break;
                    case Amqp.TraceLevel.Information:
                    case Amqp.TraceLevel.Output:
                        // 
                        // Applications should not access AmqpLite directly so there
                        // should be no 'Output' level logs.
                        Tracer.InfoFormat(format, args);
                        break;
                    case Amqp.TraceLevel.Warning:
                        Tracer.WarnFormat(format, args);
                        break;
                    case Amqp.TraceLevel.Error:
                        Tracer.ErrorFormat(format, args);
                        break;
                    default:
                        Tracer.InfoFormat("Unknown AMQP LogLevel: {}", level);
                        Tracer.InfoFormat(format, args);
                        break;
                }
            };

        }

        #endregion

        #region Connection Factory Properties

        internal bool IsClientIdSet
        {
            get => this.clientId == null;
        }

        public string ClientId
        {
            get { return this.clientId; }
            internal set
            {
                this.clientId = value;
            }
        }


        private IdGenerator ClientIDGenerator
        {
            get
            {
                IdGenerator cig = clientIdGenerator;
                lock (this)
                {
                    if (cig == null)
                    {
                        clientIdGenerator = new IdGenerator();
                        cig = clientIdGenerator;
                    }
                }
                return cig;
            }
        }

        internal Amqp.IConnectionFactory Factory { get => this.impl; }

        internal IProviderTransportContext Context { get => this.transportContext; }

        #endregion

        #region IConnection Members

        public Uri BrokerUri
        {
            get { return brokerUri; }
            set
            {
                brokerUri = value;
                if (value != null)
                {
                    amqpHost = UriUtil.ToAddress(value);
                }
                else
                {
                    amqpHost = null;
                }   
                InitTransportProperties();
                UpdateConnectionProperties();
            }
        }

        public ConsumerTransformerDelegate ConsumerTransformer
        {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public ProducerTransformerDelegate ProducerTransformer
        {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        public IRedeliveryPolicy RedeliveryPolicy
        {
            get
            {
                if (redeliveryPolicy == null)
                {
                    this.redeliveryPolicy = new RedeliveryPolicy();
                }
                return this.redeliveryPolicy;
            }
            set
            {
                if (value != null)
                {
                    this.redeliveryPolicy = value;
                }
            }
        }

        public Apache.NMS.IConnection CreateConnection()
        {
            try
            {
                Connection conn = new Connection(brokerUri, ClientIDGenerator);

                Tracer.Info("Configuring Connection Properties");

                bool shouldSetClientID = this.clientId != null;

                conn.Configure(this);

                if (shouldSetClientID)
                {
                    conn.ClientId = this.clientId;

                    conn.Connect();
                }

                return conn;

            }
            catch (Exception ex)
            {
                if (ex is NMSException)
                {
                    throw ex;
                }
                else
                {
                    throw new NMSException(ex.Message, ex);
                }
            }
        }

        public Apache.NMS.IConnection CreateConnection(string userName, string password)
        {

            if(ConnectionProperties.ContainsKey(USERNAME_PROP))
            {
                ConnectionProperties[USERNAME_PROP] = userName;
            }
            else
            {
                ConnectionProperties.Add(USERNAME_PROP, userName);
            }

            if (ConnectionProperties.ContainsKey(PASSWORD_PROP))
            {
                ConnectionProperties[PASSWORD_PROP] = password;
            }
            else
            {
                ConnectionProperties.Add(PASSWORD_PROP, password);
            }

            return CreateConnection();
        }



        #endregion
        #region AMQP Connection Properties
        public Amqp.TraceLevel AMQPlogLevel
        {
            get { return this.AMQPlogLevel; }
            set
            {
                if (null != this.transportContext)
                {
                    this.AMQPlogLevel = value;
                    Amqp.Trace.TraceLevel = value;
                }

            }
        }
        #endregion

        #region SSLConnection Methods

        public RemoteCertificateValidationCallback CertificateValidationCallback
        {
            get
            {
                return (IsSSL) ? (transportContext as IProviderSecureTransportContext).ServerCertificateValidateCallback : null;
            }
            set
            {
                if (IsSSL)
                {
                    (transportContext as IProviderSecureTransportContext).ServerCertificateValidateCallback = value;
                }
            }
        }

        public LocalCertificateSelectionCallback LocalCertificateSelect
        {
            get
            {
                return (IsSSL) ? (transportContext as IProviderSecureTransportContext).ClientCertificateSelectCallback : null;
            }
            set
            {
                if (IsSSL)
                {
                    (transportContext as IProviderSecureTransportContext).ClientCertificateSelectCallback = value;
                }
            }
        }

        public bool IsSSL
        {
            get
            {
                return amqpHost?.UseSsl ?? false;
            }
        }
        
        private void InitTransportProperties()
        {
            if (IsSSL)
            {
                SecureTransportContext stc = new SecureTransportContext(this);
                this.transportContext = stc;
            }
            else
            {
                this.transportContext = new TransportContext(this);
            }
            
            StringDictionary queryProps = URISupport.ParseParameters(this.brokerUri);
            StringDictionary transportProperties = URISupport.GetProperties(queryProps, TransportPropertyPrefix);
            if (this.applicationProperties != null)
            {
                StringDictionary appTProps = URISupport.GetProperties(this.applicationProperties, TransportPropertyPrefix);
                transportProperties = PropertyUtil.Merge(transportProperties, appTProps, string.Empty, string.Empty, TransportPropertyPrefix);
            }
            PropertyUtil.SetProperties(this.transportContext, transportProperties, TransportPropertyPrefix);
            if (IsSSL)
            {
                this.transportProperties = new SecureTransportPropertyInterceptor(this.transportContext as IProviderSecureTransportContext, transportProperties);
            }
            else
            {
                this.transportProperties = new TransportPropertyInterceptor(this.transportContext, transportProperties);
            }
        }

        private void InitApplicationProperties(StringDictionary props)
        {
            // copy properties to temporary dictionary
            StringDictionary result = PropertyUtil.Clone(props);
            // extract connections properties
            StringDictionary connProps = ExtractConnectionProperties(result);
            // initialize applications properties as the union of temp and conn properties
            this.applicationProperties = PropertyUtil.Merge(result, connProps, "", "", "");

        }

        private StringDictionary ExtractConnectionProperties(StringDictionary rawProps)
        {
            // find and extract properties with ConnectionPropertyPrefix
            StringDictionary connectionProperties = URISupport.ExtractProperties(rawProps, ConnectionPropertyPrefix);
            // find and extract properties with ConnectionPropertyAlternativePrefix
            StringDictionary connectionAlternativeProperties = URISupport.ExtractProperties(rawProps, ConnectionPropertyAlternativePrefix);
            // return Union of Conn and AltConn properties prefering Conn over AltConn.
            return PropertyUtil.Merge(connectionProperties, connectionAlternativeProperties, ConnectionPropertyPrefix, ConnectionPropertyAlternativePrefix, ConnectionPropertyPrefix);
        }

        private StringDictionary CreateConnectionProperties(StringDictionary rawProps)
        {
            // read properties with ConnectionPropertyPrefix
            StringDictionary connectionProperties = URISupport.GetProperties(rawProps, ConnectionPropertyPrefix);
            // read properties with ConnectionPropertyAlternativePrefix
            StringDictionary connectionAlternativeProperties = URISupport.GetProperties(rawProps, ConnectionPropertyAlternativePrefix);
            // return Union of Conn and AltConn properties prefering Conn over AltConn.
            return PropertyUtil.Merge(connectionProperties, connectionAlternativeProperties, ConnectionPropertyPrefix, ConnectionPropertyAlternativePrefix, ConnectionPropertyPrefix);
        }

        private void UpdateConnectionProperties()
        {
            StringDictionary queryProps = URISupport.ParseParameters(this.brokerUri);
            StringDictionary brokerConnectionProperties = CreateConnectionProperties(queryProps);
            if (this.applicationProperties != null)
            {
                // combine connection properties with application properties prefering URI properties over application
                this.properties = PropertyUtil.Merge(brokerConnectionProperties, applicationProperties, "", "", "");
            }
            else
            {
                this.properties = brokerConnectionProperties;
            }
            // update connection factory members.
            connectionProperties = new ConnectionFactoryPropertyInterceptor(this, this.properties);
        }
        #endregion

        #region Connection Factory Property Methods
        
        public StringDictionary TransportProperties
        {
            get { return this.transportProperties; }
        }

        #endregion

        #region Connection Properties Methods

        public StringDictionary ConnectionProperties
        {
            get { return this.connectionProperties; }
        }

        public bool HasConnectionProperty(string key)
        {
            return this.properties.ContainsKey(key);
        }

        #endregion
    }

    
}
