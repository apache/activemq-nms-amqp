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
using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using Apache.NMS;
using Apache.NMS.AMQP.Test.Util;
using Apache.NMS.AMQP.Test.Attribute;
using System.Security.Cryptography.X509Certificates;

namespace Apache.NMS.AMQP.Test.TestCase
{
    // initial ssl connection tests.
    [TestFixture]
    class SecureConnectionTest : BaseTestCase
    {
        protected const string TestSuiteClientCertificateFileName = "config/cert/client.crt";
        protected const string ApplicationCallbackExceptionMessage = "Bad Application";

        protected IConnection Connection;
        protected readonly static TimeSpan ConnectTimeout = TimeSpan.FromMilliseconds(60000);
        public override void Setup()
        {
            if(!this.IsSecureBroker)
            {
                Assert.Ignore("Broker {0} not configured for secure connection.", TestConfig.Instance.BrokerUri.ToString());
            }
            base.Setup();

        }

        public override void TearDown()
        {
            base.TearDown();
            Connection?.Close();
            Connection = null;
        }


        #region Useful Function

        private static void UpdateDictionaryEntry(StringDictionary dict, string key, string value)
        {
            if(dict.ContainsKey(key))
            {
                dict[key] = value;
            }
            else
            {
                dict.Add(key, value);
            }
        }

        private static List<StringDictionary> GenerateProperties(IDictionary<string, IList<string>> propertyValueSet)
        {
            List<StringDictionary> propertyCombinations = new List<StringDictionary>();
            int propertyCount = propertyValueSet.Keys.Count;
            string[] properties = new string[propertyCount];
            propertyValueSet.Keys.CopyTo(properties, 0);
            for (int i=0; i<propertyCount; i++)
            {
                
                string property = properties[i];
                IList<string> propertyValues = propertyValueSet[property];
                int valueCount = propertyValues.Count;

                foreach (string propertyValue in propertyValues)
                {
                    StringDictionary propertyCombination = new StringDictionary();

                    // add current property value

                    UpdateDictionaryEntry(propertyCombination, property, propertyValue);

                    propertyCombinations.Add(Clone(propertyCombination));

                    // adds the other properties
                    for (int j = i + 1; j < propertyCount; j++)
                    {

                        string otherProperty = properties[j];
                        IList<string> otherPropertyValues = propertyValueSet[otherProperty];
                        int otherValueCount = otherPropertyValues.Count;

                        foreach (string otherValue in otherPropertyValues)
                        {
                            UpdateDictionaryEntry(propertyCombination, otherProperty, otherValue);

                            propertyCombinations.Add(Clone(propertyCombination));

                        }

                    }
                }
            }
            return propertyCombinations;
        }

        private static string GetSecureProviderURIString()
        {
            if(TestConfig.Instance.IsSecureBroker)
            {
                return TestConfig.Instance.BrokerUri.ToString();
            }
            else
            {
                const string SecurePort = "5671";
                const string SecureScheme = "amqps://";
                string brokerIp = TestConfig.Instance.BrokerIpAddress;
                string providerURI = string.Format("{0}{1}:{2}", SecureScheme, brokerIp, SecurePort);
                return providerURI;
            }
        }


        private IConnection CreateSecureConnection(IConnectionFactory connectionFactory)
        {
            IConnection connection = null;
            if (TestConfig.Instance.BrokerUsername != null)
            {
                connection = connectionFactory.CreateConnection(TestConfig.Instance.BrokerUsername, TestConfig.Instance.BrokerPassword);
            }
            else
            {
                connection = connectionFactory.CreateConnection();
            }

            connection.RequestTimeout = ConnectTimeout;

            return connection;
        }

        private void TestSecureConnect(out Exception failure)
        {
            TestSecureConnect(Connection, out failure);
        }

        private static void TestSecureConnect(IConnection connection, out Exception failure)
        {
            failure = null;
            try
            {
                connection.Start();
            }
            catch (Exception ex)
            {
                failure = ex;
            }
        }

        #endregion

        private bool ConnectionFactoryTransportPropertiesMatch(StringDictionary expected, ConnectionFactory actualFactory, out string failure)
        {
            failure = null;
            StringDictionary actualProperties = actualFactory.TransportProperties;
            foreach(string property in expected.Keys)
            {
                if (actualProperties.ContainsKey(property))
                {
                    string expectedValue = expected[property];
                    string actualValue = actualProperties[property];
                    if (expectedValue != null && actualValue != null)
                    {
                        if (String.Compare(expectedValue, actualValue, true) != 0)
                        {
                            failure = string.Format("Connection Factory property {0} value {1} does not match expected value {2}", property, actualValue, expectedValue);
                            return false;
                        }
                    }
                    else if (expectedValue == null && actualValue == null)
                    {
                        // matches ignore
                    }
                    else 
                    {
                        failure = string.Format("Expected property \"{0}\" value of {1} when actual value is {2}", property, expectedValue ?? "null", actualValue ?? "null");
                        return false;
                    }
                    
                }
                else
                {
                    failure = string.Format("Connection Factory does not contain expected property {0}", property);
                    return false;
                }
            }
            return true;
        }

        private void TestConnectionFactorySSLPropertyConfiguration(StringDictionary props)
        {
            try
            {
                string providerUri = GetSecureProviderURIString();

                ConnectionFactory connectionFactory = new ConnectionFactory(new Uri(providerUri), props);

                string failure = null;

                Assert.IsTrue(
                    ConnectionFactoryTransportPropertiesMatch(props, connectionFactory, out failure),
                    "Transport Properties do not match given properties. Cause : {0}",
                    failure ?? ""
                    );
            }
            catch (Exception ex)
            {
                this.PrintTestFailureAndAssert(
                    this.GetTestMethodName() + "TestConnectionFactorySSLPropertyConfiguration", 
                    "Unexpected Exception", 
                    ex
                    );
            }
        }

        private void TestConnectionFactorySSLURIConfiguration(StringDictionary props)
        {
            try
            {
                // create provider URI
                string providerUriQueryParameters = Apache.NMS.Util.URISupport.CreateQueryString(props);
                string providerUriBase = GetSecureProviderURIString();
                string providerUri = string.Format("{0}?{1}", providerUriBase, providerUriQueryParameters);

                IConnectionFactory connectionFactory = CreateConnectionFactory();
                connectionFactory.BrokerUri = new Uri(providerUri);
                ConnectionFactory providerConnectionFactory = connectionFactory as ConnectionFactory;

                string failure = null;
                
                Assert.IsTrue(
                    ConnectionFactoryTransportPropertiesMatch(props, providerConnectionFactory, out failure), 
                    "Transport Properties do not match URI parameters {0}. Cause : {1}", 
                    providerUriQueryParameters, 
                    failure ?? ""
                    );

            }
            catch (Exception ex)
            {
                this.PrintTestFailureAndAssert(
                    this.GetTestMethodName() + "_TestConnectionFactorySSLURIConfiguration", 
                    "Unexpected Exception", 
                    ex
                    );
            }
        }

        /*
         * Tests how to configure ssl options for a connection factory, via URI and properties.
         * These properties are verfified using the Provider connection factory property TransportProperties.
         */
        [Test]
        public void TestConnectionFactorySSLConfiguration()
        {
            /*
             * PropertyTestValues is the set of all secure transport properties and values to combine and 
             * assign using the connectionfactory URI or the StringDictionary parameter in the connection
             * Factory constructor. This will be used to generate the combinations of properties and values.
             */
            IDictionary<string, IList<string>> PropertyTestValues = new Dictionary<string, IList<string>>()
            {
                {
                    NMSPropertyConstants.NMS_SECURE_TANSPORT_SSL_PROTOCOLS,
                    /* for simplicity only string that can be produced by SSLProtocols.ToString are used */
                    new List<string> { "Default", "Tls, Tls11, Tls12" }
                },
                {
                    NMSPropertyConstants.NMS_SECURE_TRANSPORT_ACCEPT_INVALID_BROKER_CERT,
                    new List<string> { bool.TrueString, bool.FalseString }
                },
                {
                    NMSPropertyConstants.NMS_SECURE_TRANSPORT_CLIENT_CERT_FILE_NAME,
                    new List<string> { TestSuiteClientCertificateFileName, "foo.p12", "testfile" }
                },
                {
                    NMSPropertyConstants.NMS_SECURE_TANSPORT_SERVER_NAME,
                    new List<string>() { "localhost", "test with spaces" }
                },
                {
                    NMSPropertyConstants.NMS_SECURE_TANSPORT_CLIENT_CERT_SUBJECT,
                    new List<string>() { "a subject one.", "CN=client", "myself" }
                },
                {
                    NMSPropertyConstants.NMS_SECURE_TANSPORT_CLIENT_CERT_PASSWORD,
                    new List<string>() { "password", "abcABC123/*-+", "test with spaces" }
                },
                {
                    NMSPropertyConstants.NMS_SECURE_TANSPORT_KEY_STORE_LOCATION,
                    new List<string>
                    {
                        StoreLocation.CurrentUser.ToString(),
                        StoreLocation.LocalMachine.ToString()
                    }
                },
                {
                    NMSPropertyConstants.NMS_SECURE_TANSPORT_KEY_STORE_NAME,
                    new List<string>
                    {
                        StoreName.My.ToString(),
                        StoreName.AddressBook.ToString(),
                        StoreName.AuthRoot.ToString(),
                        StoreName.CertificateAuthority.ToString(),
                        StoreName.Disallowed.ToString(),
                        StoreName.Root.ToString(),
                        StoreName.TrustedPeople.ToString(),
                        StoreName.TrustedPublisher.ToString(),
                        "other"
                    }
                },
                {
                    NMSPropertyConstants.NMS_SECURE_TRANSPORT_SSL_CHECK_CERTIFICATE_REVOCATION,
                    new List<string>
                    {
                        bool.TrueString,
                        bool.FalseString
                    }
                }
            };

            Logger.Info("Generating SSL configuration Test properties.");

            List<StringDictionary> TestConfigurations = GenerateProperties(PropertyTestValues);

            Logger.Info(string.Format("Generated {0} property configurations.", TestConfigurations.Count));

            int count = 0;

            foreach(StringDictionary configuration in TestConfigurations)
            {
                count++;
                if(Logger.IsDebugEnabled)
                {
                    Logger.Debug(string.Format("Testing SSL configuration combination {0} : \n{1}", count, ToString(configuration)));
                }

                TestConnectionFactorySSLPropertyConfiguration(configuration);

                TestConnectionFactorySSLURIConfiguration(configuration);
            }
        }

        /*
         * Tests valid values for the transport.SSLProtocol property.
         * The test then attempts to connect to the broker with the assigned property creating should it not be able to connect.
         * The protocols Ssl3, Ssl2 and tls1.0 are not secured so all test values should result in the tsl11, or later, protocol to 
         * be selected should a broker support the secure protocols.
         * */
        [Test]
        public void TestValidSSLProtocols(
            [Values( "tls11", "tls12", "Ssl3,tls11", "tls,tls11", "Default,tls11", "tls,ssl3,tls11", "  tls12,tls", "tls,tls11,tls12")]
            string protocolsString
            )
        {
            StringDictionary queryOptions = new StringDictionary()
            {
                { NMSPropertyConstants.NMS_SECURE_TANSPORT_SSL_PROTOCOLS, protocolsString }
            };

            string normalizedName;
            System.Security.Authentication.SslProtocols protocol = System.Security.Authentication.SslProtocols.None;
            if (Enum.TryParse(protocolsString, true, out protocol))
            {
                normalizedName = protocol.ToString();
            }
            else
            {
                normalizedName = protocolsString;
            }


            try
            {
                // create provider URI
                string providerUriQueryParameters = Apache.NMS.Util.URISupport.CreateQueryString(queryOptions);
                string providerUriBase = GetSecureProviderURIString();
                string providerUri = string.Format("{0}?{1}", providerUriBase, providerUriQueryParameters);

                IConnectionFactory connectionFactory = CreateConnectionFactory();
                connectionFactory.BrokerUri = new Uri(providerUri);
                ConnectionFactory providerConnectionFactory = connectionFactory as ConnectionFactory;
                // disables certificate validation
                providerConnectionFactory.CertificateValidationCallback = (a, b, c, d) => true;
                string transportSSLProtocol = providerConnectionFactory.TransportProperties[NMSPropertyConstants.NMS_SECURE_TANSPORT_SSL_PROTOCOLS];

                Assert.AreEqual(normalizedName, transportSSLProtocol);

                Connection = CreateSecureConnection(connectionFactory);
                
                try
                {
                    // attempt to connect to broker
                    Connection.Start();
                }
                catch (NMSSecurityException secEx)
                {
                    Logger.Warn(string.Format("Security failure. Check {0} file for test configuration. Or check broker configuration. Security Message : {1}", Configuration.CONFIG_FILENAME, secEx.Message));
                }
            }
            catch(Exception ex)
            {
                this.PrintTestFailureAndAssert(this.GetTestMethodName(), "Unexpected Exception", ex);
            }
            finally
            {
                Connection?.Close();
            }
            
        }


        [Test]
        public void TestProviderRemoteCertificateValidationCallback()
        {
            StringDictionary queryOptions = new StringDictionary()
            {
                { NMSPropertyConstants.NMS_SECURE_TRANSPORT_ACCEPT_INVALID_BROKER_CERT, bool.TrueString }
            };
            try
            {
                // Create Provider URI
                string providerUriQueryParameters = Apache.NMS.Util.URISupport.CreateQueryString(queryOptions);
                string providerUriBase = GetSecureProviderURIString();
                string providerUri = string.Format("{0}?{1}", providerUriBase, providerUriQueryParameters);

                // Create Provider connection factory
                IConnectionFactory connectionFactory = CreateConnectionFactory();
                connectionFactory.BrokerUri = new Uri(providerUri);
                ConnectionFactory providerConnectionFactory = connectionFactory as ConnectionFactory;

                bool signaled = false;

                // (object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
                // test Validation Callback can override NMS_SECURE_TRANSPORT_ACCEPT_INVALID_BROKER_CERT property
                providerConnectionFactory.CertificateValidationCallback = (sender, cert, chain, errors) =>
                {
                    signaled = true;
                    // indicate that the remote certificate is invalid.
                    return false;
                };

                Exception connectionFailure = null;

                // Create connection using NMS Interface
                using (Connection = CreateSecureConnection(connectionFactory))
                {
                    TestSecureConnect(Connection, out connectionFailure);

                    Assert.IsTrue(signaled, "Application callback was not executed.");

                    Assert.NotNull(connectionFailure, "Application callback CertificateValidationCallback should cause Connection start to fail. Expected exception.");

                    Logger.Info(string.Format("Caught exception on invalid start of connection. Connection failure cause : {0} ", connectionFailure));
                }

                Connection = null;
                connectionFailure = null;
                signaled = false;

                //update connection factory transport settings

                providerConnectionFactory.TransportProperties[NMSPropertyConstants.NMS_SECURE_TRANSPORT_ACCEPT_INVALID_BROKER_CERT] = bool.FalseString;

                // Test NMS_SECURE_TRANSPORT_ACCEPT_INVALID_BROKER_CERT is ignored when the application callback throws exception.
                providerConnectionFactory.CertificateValidationCallback = (sender, cert, chain, errors) =>
                {
                    signaled = true;
                    // throw dummy exception to indicate an application callback failure.
                    throw new Exception(ApplicationCallbackExceptionMessage);
                };

                // Create connection using NMS Interface
                using (Connection = CreateSecureConnection(connectionFactory))
                {
                    TestSecureConnect(Connection, out connectionFailure);

                    Assert.IsTrue(signaled, "Application Callback thats throws exception was not executed.");

                    Assert.NotNull(connectionFailure, "Connection did not produce an exception from the application callback.");

                    Assert.AreEqual(ApplicationCallbackExceptionMessage, connectionFailure.InnerException?.Message, "Exception produced was not application callback exception.");
                }
            }
            catch (Exception ex)
            {
                this.PrintTestFailureAndAssert(this.GetTestMethodName(), "Unexpected Exception", ex);
            }
        }

        /*
         * Tests that an application can load an X509Certificate and use that certificate in the callback.
         * This test can fail different base on the TestSuite configuration. Should the testSuite be configured 
         * to use a client certificate this test assumes that the remote broker can accept the client certificate 
         * and fail appropriately. Should the test suite not be configured to use a client certificate the test 
         * will not fail on a connection failure.
         */
        [Test]
        public void TestProviderLocalCertificateSelectCallback()
        {
            bool isConfiguredCert = !String.IsNullOrWhiteSpace(TestConfig.Instance.ClientCertFileName);
            string clientCertFileName = isConfiguredCert ? TestConfig.Instance.ClientCertFileName : TestSuiteClientCertificateFileName;
            // configure to ignore remote certificate validation.
            StringDictionary queryOptions = new StringDictionary()
            {
                { NMSPropertyConstants.NMS_SECURE_TRANSPORT_ACCEPT_INVALID_BROKER_CERT, bool.TrueString }
            };
            try
            {
                // load certificate
                X509Certificate cert = null;
                try
                {
                     cert = X509Certificate2.CreateFromCertFile(clientCertFileName);
                }
                catch (Exception ex)
                {
                    // failure to load should fail test.
                    Assert.Fail("Could not load client certificate for from file {0}, cause of failure : {1}", clientCertFileName, ex.Message);
                }

                Assert.NotNull(cert, "Failed to load client certificate {0}.", clientCertFileName);

                // Create Provider URI
                string providerUriQueryParameters = Apache.NMS.Util.URISupport.CreateQueryString(queryOptions);
                string providerUriBase = GetSecureProviderURIString();
                string providerUri = string.Format("{0}?{1}", providerUriBase, providerUriQueryParameters);

                // Create Provider connection factory
                IConnectionFactory connectionFactory = CreateConnectionFactory();
                connectionFactory.BrokerUri = new Uri(providerUri);
                ConnectionFactory providerConnectionFactory = connectionFactory as ConnectionFactory;

                bool signaled = false;
                Exception connectionFailure = null;

                // Test selecting certificate from loaded by application

                // X509Certificate LocalCertificateSelectionCallback(object sender, string targetHost, X509CertificateCollection localCertificates, X509Certificate remoteCertificate, string[] acceptableIssuers)
                providerConnectionFactory.LocalCertificateSelect = (sender, targetHost, localCertificates, remoteCertificate, issuers) =>
                {
                    signaled = true;
                    // return loaded certificate
                    return cert;
                };

                using (Connection = CreateSecureConnection(connectionFactory))
                {

                    TestSecureConnect(Connection, out connectionFailure);

                    string failureCause = null;
                    // log any failure for reference
                    if (connectionFailure != null)
                    {
                        failureCause = string.Format("Connection failed to connection. Cause : {0}", connectionFailure.Message);
                        Logger.Info("Caught exception for secure connection " + connectionFailure);
                    }

                    Assert.IsTrue(signaled, "LocalCertificateSelectCallback failed to execute. {0}", failureCause ?? "");
                    
                    // Should the runner of the test suite indent to use a configured client certificate
                    // the test should fail if the connection fails to connection.
                    // This means the client certificate selected must be accepted by the remote broker.
                    if (isConfiguredCert)
                    {
                        Assert.IsNull(connectionFailure, "Caught Exception for configured client certificate." +
                            " Please check TestSuite Configuration in {0} and" +
                            " ensure host {1} can process client certificate {2}." +
                            " Failure Message : {3}",
                            Configuration.CONFIG_FILENAME, 
                            TestConfig.Instance.BrokerIpAddress, 
                            clientCertFileName,
                            connectionFailure?.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                this.PrintTestFailureAndAssert(this.GetTestMethodName(), "Unexpected Exception", ex);
            }
        }

        /*
         * Tests the behaviour of the application LocalCertificateSelectCallback callback throw an exception.
         */
        [Test]
        public void TestProviderLocalCertificateSelectCallbackThrowsException()
        {
            // configure to ignore remote certificate validation.
            StringDictionary queryOptions = new StringDictionary()
            {
                { NMSPropertyConstants.NMS_SECURE_TRANSPORT_ACCEPT_INVALID_BROKER_CERT, bool.TrueString }
            };
            try
            {
                // Create Provider URI
                string providerUriQueryParameters = Apache.NMS.Util.URISupport.CreateQueryString(queryOptions);
                string providerUriBase = GetSecureProviderURIString();
                string providerUri = string.Format("{0}?{1}", providerUriBase, providerUriQueryParameters);

                // Create Provider connection factory
                IConnectionFactory connectionFactory = CreateConnectionFactory();
                connectionFactory.BrokerUri = new Uri(providerUri);
                ConnectionFactory providerConnectionFactory = connectionFactory as ConnectionFactory;

                bool signaled = false;
                Exception connectionFailure = null;

                providerConnectionFactory.LocalCertificateSelect = (sender, targetHost, localCertificates, remoteCertificate, issuers) =>
                {
                    signaled = true;
                    // throw exception
                    throw new Exception(ApplicationCallbackExceptionMessage);
                };

                using (Connection = CreateSecureConnection(connectionFactory))
                {
                    TestSecureConnect(Connection, out connectionFailure);

                    string failureCause = null;
                    // log any failure for reference
                    if (connectionFailure != null)
                    {
                        failureCause = string.Format("Connection failed to connection. Cause : {0}", connectionFailure.Message);
                        Logger.Info("Caught exception for secure connection " + connectionFailure);
                    }

                    Assert.IsTrue(signaled, "LocalCertificateSelectCallback failed to execute. {0}", failureCause ?? "");

                    Assert.NotNull(connectionFailure, "Connection did not produce exception from application callback.");

                    Assert.AreEqual(ApplicationCallbackExceptionMessage, connectionFailure.InnerException?.Message, "Exception produced was not application callback exception.");
                }
            }
            catch (Exception ex)
            {
                this.PrintTestFailureAndAssert(this.GetTestMethodName(), "Unexpected Exception", ex);
            }
        }

        /*
         * Tests that the property for client certificate file name loads the certificate into the
         * certificate collection for selection.
         */
        [Test]
        public void TestClientCertificateFileName()
        {
            string clientCertificateFileName = TestSuiteClientCertificateFileName;
            // Configure to ignore remote certificate validation and add TestSuiteClientCertificateFileName for selction.
            StringDictionary queryOptions = new StringDictionary()
            {
                { NMSPropertyConstants.NMS_SECURE_TRANSPORT_ACCEPT_INVALID_BROKER_CERT, bool.TrueString },
                { NMSPropertyConstants.NMS_SECURE_TRANSPORT_CLIENT_CERT_FILE_NAME, clientCertificateFileName }
            };

            try
            {
                // load certificate
                X509Certificate cert = null;
                try
                {
                    cert = X509Certificate2.CreateFromCertFile(clientCertificateFileName);
                }
                catch (Exception ex)
                {
                    // failure to load should fail test.
                    Assert.Fail(
                        "Could not load client certificate for from file {0}, cause of failure : {1}", 
                        clientCertificateFileName, 
                        ex.Message
                        );
                }
                Assert.NotNull(cert, "Failed to load client certificate. {0}", clientCertificateFileName);

                byte[] clientCertBytes = cert.GetRawCertData();

                // Create Provider URI
                string providerUriQueryParameters = Apache.NMS.Util.URISupport.CreateQueryString(queryOptions);
                string providerUriBase = GetSecureProviderURIString();
                string providerUri = string.Format("{0}?{1}", providerUriBase, providerUriQueryParameters);

                // Create Provider connection factory
                IConnectionFactory connectionFactory = CreateConnectionFactory();
                connectionFactory.BrokerUri = new Uri(providerUri);
                ConnectionFactory providerConnectionFactory = connectionFactory as ConnectionFactory;

                Exception connectionFailure = null;
                bool foundCert = false;

                // use the LocalCertificateSelect callback to test the localCertificates parameter for the client certificate.
                providerConnectionFactory.LocalCertificateSelect = (sender, targetHost, localCertificates, remoteCertificate, issuers) =>
                {
                    X509Certificate selected = null;
                    foreach (X509Certificate localCert in localCertificates)
                    {
                        byte[] localCertBytes = localCert.GetRawCertData();
                        if (localCertBytes.Length == clientCertBytes.Length)
                        {
                            int bytesMatched = 0;
                            for (int i=0; i<localCertBytes.Length; i++)
                            {

                                if (localCertBytes[i] != clientCertBytes[i])
                                {
                                    break;
                                }
                                bytesMatched++;
                            }
                            if(bytesMatched == clientCertBytes.Length)
                            {
                                foundCert = true;
                                selected = localCert;
                                break;
                            }
                        }
                    }
                    return selected;
                };

                using(Connection = CreateSecureConnection(connectionFactory))
                {
                    // Connect to invoke callback to test.
                    TestSecureConnect(Connection, out connectionFailure);

                    // log connection failure for reference or debugging.
                    if (connectionFailure != null)
                    {
                        Logger.Info("Caught exception for secure connection " + connectionFailure);
                    }

                    Assert.IsTrue(foundCert, "Could not find certificate {0} when added using property \"{1}\"", clientCertificateFileName, NMSPropertyConstants.NMS_SECURE_TRANSPORT_CLIENT_CERT_FILE_NAME);
                }

            }
            catch (Exception ex)
            {
                this.PrintTestFailureAndAssert(this.GetTestMethodName(), "Unexpected Exception", ex);
            }
        }

        /*
         * Tests the NMSPropertyConstants.NMS_SECURE_TRANSPORT_ACCEPT_INVALID_BROKER_CERT property for 
         * both Values bool.TrueString and bool.FalseString. Should the true value be used the connection 
         * should not be able to throw an exception to due a validation error. Should the false value be 
         * used the connection may or not may not throw an exception depending on the broker's response.
         */
        [Test]
        public void TestAcceptInvalidServerCertificateProperty(
            [Values(true, false)]
            bool accepted
            )
        {
            StringDictionary props = new StringDictionary()
            {
                {
                    NMSPropertyConstants.NMS_SECURE_TRANSPORT_ACCEPT_INVALID_BROKER_CERT,
                    accepted ? bool.TrueString : bool.FalseString
                }
            };

            try
            {
                // Create Provider URI
                string providerUriQueryParameters = Apache.NMS.Util.URISupport.CreateQueryString(props);
                string providerUriBase = GetSecureProviderURIString();
                string providerUri = string.Format("{0}?{1}", providerUriBase, providerUriQueryParameters);

                // Create Provider connection factory
                this.InitConnectedFactoryProperties(props);
                
                IConnectionFactory connectionFactory = CreateConnectionFactory();
                connectionFactory.BrokerUri = new Uri(providerUri);
                ConnectionFactory providerConnectionFactory = connectionFactory as ConnectionFactory;

                Exception connectionFailure = null;

                using (Connection = CreateSecureConnection(connectionFactory))
                {
                    TestSecureConnect(Connection, out connectionFailure);

                    // log connection failure for reference or debugging.
                    if (connectionFailure != null)
                    {
                        Logger.Info("Caught exception for secure connection " + connectionFailure);
                    }

                    /*
                     * Assert null exception on accepted to indicate any ssl errors were supressed.
                     * The connection may fail or not depending broker's response.
                     */
                    if (accepted)
                    {
                        Assert.IsNull(connectionFailure, "Connection failed to connect. Cause {0}", connectionFailure?.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                this.PrintTestFailureAndAssert(this.GetTestMethodName(), "Unexpected Exception", ex);
            }
        }
    }
}
