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
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Handler;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Transport
{

    /// <summary>
    /// Secure Transport management is mainly handled by the AmqpNetLite library, Except for certicate selection and valibdation.
    /// SecureTransportContext should configure the Amqp.ConnectionFactory for the ssl transport properties.
    /// </summary>
    internal class SecureTransportContext : TransportContext, ISecureTransportContext
    {
        
        private static readonly List<string> SupportedProtocols;
        private static readonly Dictionary<string, int> SupportedProtocolValues;

        #region static Initializer

        static SecureTransportContext()
        {
            const string Default = "Default";
            const string None = "None";
            SupportedProtocols = new List<string>();
            SupportedProtocolValues = new Dictionary<string, int>();
            foreach (string name in Enum.GetNames(typeof(System.Security.Authentication.SslProtocols)))
            {
                if (name.Equals(Default, StringComparison.CurrentCultureIgnoreCase) ||
                   name.Equals(None, StringComparison.CurrentCultureIgnoreCase))
                {
                    // ignore
                }
                else
                {
                    SupportedProtocols.Add(name);
                }

            }
            foreach (int value in Enum.GetValues(typeof(System.Security.Authentication.SslProtocols)))
            {
                SslProtocols p = (System.Security.Authentication.SslProtocols)value;
                if (p.Equals(SslProtocols.Default) ||
                   p.Equals(SslProtocols.None))
                {
                    // ignore
                }
                else 
                {
                    string name = ((SslProtocols)value).ToString().ToLower();
                    SupportedProtocolValues.Add(name, value);
                }
            }
            if (Tracer.IsDebugEnabled)
            {
                Tracer.DebugFormat("Supported SSL protocols list {0}", Util.PropertyUtil.ToString(SupportedProtocols));
            }
        }

        #endregion

        
        #region Constructors

        internal SecureTransportContext() : base()
        {
            this.connectionBuilder.SSL.LocalCertificateSelectionCallback = this.ContextLocalCertificateSelect;
            this.connectionBuilder.SSL.RemoteCertificateValidationCallback = this.ContextServerCertificateValidation;
            connectionBuilder.SASL.Profile = Amqp.Sasl.SaslProfile.Anonymous;
        }

        // Copy Contructor

        #endregion

        #region Secure Transport Context Properties

        public string KeyStoreName { get; set; }

        public string KeyStorePassword { get; set; }

        public string ClientCertFileName { get; set; }

        public bool AcceptInvalidBrokerCert { get; set; } = false;
        
        public string ClientCertSubject { get; set; }
        public string ClientCertPassword { get; set; }
        public string KeyStoreLocation { get; set; }
        public string SSLProtocol
        {
            get
            {
                return this.connectionBuilder?.SSL.Protocols.ToString();
            }
            set
            {
                this.connectionBuilder.SSL.Protocols = GetSslProtocols(value);
            }
        }

        public bool CheckCertificateRevocation
        {
            get
            {
                return this.connectionBuilder?.SSL.CheckCertificateRevocation ?? false;
            }
            set
            {
                if(this.connectionBuilder != null)
                    this.connectionBuilder.SSL.CheckCertificateRevocation = value;
            }
        }

        public override bool IsSecure { get; } = true;

        public string ServerName { get; set; }

        public RemoteCertificateValidationCallback ServerCertificateValidateCallback { get; set; }
        public LocalCertificateSelectionCallback ClientCertificateSelectCallback { get; set; }

        #endregion

        #region Private Methods
        // These are the default values given by amqpnetlite.
        private static readonly SslProtocols DefaultProtocols = (new Amqp.ConnectionFactory()).SSL.Protocols;


        private SslProtocols GetSslProtocols(string protocolString)
        {
            
            if (!String.IsNullOrWhiteSpace(protocolString))
            {
                SslProtocols value = DefaultProtocols;
                if(Enum.TryParse(protocolString, true, out value))
                {
                    return value;
                }
                else
                {
                    throw new InvalidPropertyException(SecureTransportPropertyInterceptor.SSL_PROTOCOLS_PROPERTY, string.Format("Failed to parse value {0}", protocolString));
                }
            }
            else
            {
                return DefaultProtocols;
            }
            
        }

        private X509Certificate2Collection LoadClientCertificates()
        {
            X509Certificate2Collection certificates = new X509Certificate2Collection();

            if(!String.IsNullOrWhiteSpace(this.ClientCertFileName))
            {
                Tracer.DebugFormat("Attempting to load Client Certificate file: {0}", this.ClientCertFileName);
                X509Certificate2 certificate = new X509Certificate2(this.ClientCertFileName, this.ClientCertPassword);
                Tracer.DebugFormat("Loaded Client Certificate: {0}", certificate.Subject);

                certificates.Add(certificate);
            }
            else
            {
                string storeName = String.IsNullOrWhiteSpace(this.KeyStoreName) ? StoreName.My.ToString() : this.KeyStoreName;
                StoreLocation storeLocation = StoreLocation.CurrentUser;
                if(!String.IsNullOrWhiteSpace(this.KeyStoreLocation))
                {
                    bool found = false;
                    foreach(string location in Enum.GetNames(typeof(StoreLocation)))
                    {
                        if(String.Compare(this.KeyStoreLocation, location, true) == 0)
                        {
                            storeLocation = (StoreLocation)Enum.Parse(typeof(StoreLocation), location, true);
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                    {
                        throw new NMSException(string.Format("Invalid Store location {0}", this.KeyStoreLocation), NMSErrorCode.PROPERTY_ERROR);
                    }
                }

                Tracer.DebugFormat("Loading store {0}, from location {1}.", storeName, storeLocation.ToString());
                try
                {
                    X509Store store = new X509Store(storeName, storeLocation);

                    store.Open(OpenFlags.ReadOnly);
                    X509Certificate2[] storeCertificates = new X509Certificate2[store.Certificates.Count];
                    store.Certificates.CopyTo(storeCertificates, 0);
                    certificates.AddRange(storeCertificates);
                }
                catch(Exception ex)
                {
                    Tracer.WarnFormat("Error loading KeyStore, name : {0}; location : {1}. Cause {2}", storeName, storeLocation, ex);
                    throw ExceptionSupport.Wrap(ex, "Error loading KeyStore.", storeName, storeLocation.ToString());
                }
            }

            return certificates;
        }
        
        #endregion

        #region IProviderSecureTransportContext Methods

        public override Task<Amqp.Connection> CreateAsync(Address address, IHandler handler)
        {
            // Load local certificates
            this.connectionBuilder.SSL.ClientCertificates.AddRange(LoadClientCertificates());
            Tracer.DebugFormat("Loading Certificates from {0} possibilit{1}.", this.connectionBuilder.SSL.ClientCertificates.Count, (this.connectionBuilder.SSL.ClientCertificates.Count == 1) ? "y" : "ies");

            // log assigned SSL protocols
            Tracer.DebugFormat("Set accepted SSL protocols to {0}.", this.SSLProtocol);

            if (this.connectionBuilder.SSL.Protocols == SslProtocols.None)
            {
                throw new NMSSecurityException(string.Format("Invalid SSL Protocol {0} selected from system supported protocols {1}", this.SSLProtocol, PropertyUtil.ToString(SupportedProtocols)));
            }
            
            return base.CreateAsync(address, handler);
        }

        #endregion

        #region Certificate Callbacks

        protected X509Certificate ContextLocalCertificateSelect(object sender, string targetHost, X509CertificateCollection localCertificates, X509Certificate remoteCertificate, string[] acceptableIssuers)
        {
            if (Tracer.IsDebugEnabled)
            {
                string subjects = "{";
                string issuers = "{";
                string acceptedIssuers = "{";

                foreach (X509Certificate cert in localCertificates)
                {
                    subjects += cert.Subject + ", ";
                    issuers += cert.Issuer + ", ";
                }

                subjects += "}";
                issuers += "}";

                for (int i = 0; i < acceptableIssuers.Length; i++)
                {
                    acceptedIssuers += acceptableIssuers[i] + ", ";
                }

                Tracer.DebugFormat("Local Certificate Selection.\n" +
                    "Sender {0}, Target Host {1}, Remote Cert Subject {2}, Remote Cert Issuer {3}" +
                    "\nlocal Cert Subjects {4}, " +
                    "\nlocal Cert Issuers {5}",
                    sender.ToString(),
                    targetHost,
                    remoteCertificate?.Subject,
                    remoteCertificate?.Issuer,
                    subjects,
                    issuers);
            }
            X509Certificate localCertificate = null;
            if (ClientCertificateSelectCallback != null)
            {
                try
                {
                    if (Tracer.IsDebugEnabled) Tracer.DebugFormat("Calling application callback for Local certificate selection.");
                    localCertificate = ClientCertificateSelectCallback(sender, targetHost, localCertificates, remoteCertificate, acceptableIssuers);
                }
                catch (Exception ex)
                {
                    Tracer.InfoFormat("Caught Exception from application callback for local certificate selction. Exception : {0}", ex);
                    throw ex;
                }
            }
            else if (localCertificates.Count >= 1)
            {
                // when there is only one certificate select that certificate.
                localCertificate = localCertificates[0];
                if (!String.IsNullOrWhiteSpace(this.ClientCertSubject))
                {
                    // should the application identify a specific certificate to use search for that certificate.
                    localCertificate = null;
                    foreach (X509Certificate cert in localCertificates)
                    {
                        if (String.Compare(cert.Subject, this.ClientCertSubject, true) == 0)
                        {
                            localCertificate = cert;
                            break;
                        }
                    }
                    
                }
            }

            if (localCertificate == null)
            {
                Tracer.InfoFormat("Could not select Local Certificate for target host {0}", targetHost);
            }
            else if (Tracer.IsDebugEnabled)
            {
                Tracer.DebugFormat("Selected Local Certificate {0}", localCertificate.ToString());
            }

            return localCertificate;
        }

        protected bool ContextServerCertificateValidation(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {

            if (Tracer.IsDebugEnabled)
            {
                string name = null;
                if (certificate is X509Certificate2)
                {
                    X509Certificate2 cert = certificate as X509Certificate2;
                    name = cert.SubjectName.Name;


                }
                Tracer.DebugFormat("Cert DN {0}; Cert Subject {1}; Cert Issuer {2}; SSLPolicyErrors [{3}]", name, certificate?.Subject ?? "null", certificate?.Issuer ?? "null", sslPolicyErrors.ToString());
                try
                {
                    X509VerificationFlags verFlags = chain.ChainPolicy.VerificationFlags;
                    X509RevocationMode revMode = chain.ChainPolicy.RevocationMode;
                    X509RevocationFlag revFlags = chain.ChainPolicy.RevocationFlag;
                    StringBuilder sb = new StringBuilder();
                    sb.Append("ChainStatus={");
                    int size = sb.Length;
                    foreach (X509ChainStatus status in chain.ChainStatus)
                    {
                        X509ChainStatusFlags csflags = status.Status;
                        sb.AppendFormat("Info={0}; flags=0x{1:X}; flagNames=[{2}]", status.StatusInformation, csflags, csflags.ToString());
                        sb.Append(", ");
                    }
                    if (size != sb.Length)
                    {
                        sb.Remove(sb.Length - 2, 2);
                    }
                    sb.Append("}");

                    Tracer.DebugFormat("X.509 Cert Chain, Verification Flags {0:X} {1}, Revocation Mode {2}, Revocation Flags {3}, Status {4} ",
                        verFlags, verFlags.ToString(), revMode.ToString(), revFlags.ToString(), sb.ToString());
                }
                catch (Exception ex)
                {
                    Tracer.ErrorFormat("Error displaying Remote Cert fields. Cause: {0}", ex);
                }
            }

            bool? valid = null;
            if (ServerCertificateValidateCallback != null)
            {
                try
                {
                    if (Tracer.IsDebugEnabled) Tracer.DebugFormat("Calling application callback for Remote Certificate Validation.");
                    valid = ServerCertificateValidateCallback(sender, certificate, chain, sslPolicyErrors);
                }
                catch (Exception ex)
                {
                    Tracer.InfoFormat("Caught Exception from application callback for Remote Certificate Validation. Exception : {0}", ex);
                    throw ex;
                }
            }
            else 
            {
                if ((sslPolicyErrors & SslPolicyErrors.RemoteCertificateNameMismatch) == SslPolicyErrors.RemoteCertificateNameMismatch
                   && !String.IsNullOrWhiteSpace(this.ServerName))
                {
                    if (certificate.Subject.IndexOf(string.Format("CN={0}",
                    this.ServerName), StringComparison.InvariantCultureIgnoreCase) > -1)
                    {
                        sslPolicyErrors &= ~(SslPolicyErrors.RemoteCertificateNameMismatch);
                    }
                }
                if (sslPolicyErrors == SslPolicyErrors.None)
                {
                    valid = true;
                }
                else
                {
                    Tracer.WarnFormat("SSL certificate {0} validation error : {1}", certificate.Subject, sslPolicyErrors.ToString());
                    valid = this.AcceptInvalidBrokerCert;
                }
            }
            return valid ?? this.AcceptInvalidBrokerCert;
        }

        #endregion
    }

}
