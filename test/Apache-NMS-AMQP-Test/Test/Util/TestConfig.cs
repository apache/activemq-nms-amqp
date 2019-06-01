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
using System.IO;
using System.Xml.Serialization;

namespace Apache.NMS.AMQP.Test.Util
{
    public class TestConfig
    {
        public const string DEFAULT_BROKER_IP_ADDRESS = "127.0.0.1";
        public const string DEFAULT_BROKER_ADDRESS_SCHEME = "amqp://";
        public const string SECURE_BROKER_ADDRESS_SCHEME = "amqps://";
        public const string DEFAULT_BROKER_PORT = "5672";
        public const string DEFAULT_LOG_LEVEL = "warn";
        public const int DEFAULT_DATA_GENERATED_CHUNK_SIZE = 512 * 1024; // 512KiB

        public string AddressScheme { get => config?.Broker?.Scheme ?? DEFAULT_BROKER_ADDRESS_SCHEME; }
        public string BrokerIpAddress { get => config?.Broker?.IPAddress ?? DEFAULT_BROKER_IP_ADDRESS; }
        public string BrokerPort { get => config?.Broker?.Port ?? DEFAULT_BROKER_PORT; }
        public string BrokerUsername { get => config?.Broker?.Client?.Username; }
        public string BrokerPassword { get => config?.Broker?.Client?.Password; }

        public string ClientId { get => config?.Broker?.Client?.ClientId; }

        #region Secure Client Properties

        public string ClientCertSubject { get => config?.Broker?.Client?.ClientCertSubject; }

        public string ClientCertPassword { get => config?.Broker?.Client?.ClientCertPasword; }

        public string ClientCertFileName { get => config?.Broker?.Client?.ClientCertFileName; }

        public bool AcceptInvalidBrokerCert { get => config?.Broker?.Client?.AcceptInvalidServerCert ?? true; }

        public string KeyStoreName { get => config?.Broker?.Client?.KeyStoreName; }

        public string KeyStoreLocation { get => config?.Broker?.Client?.KeyStoreLocation; }

        public string BrokerName { get => config?.Broker?.Name; }

        #endregion

        protected Uri uri = null; 
        public Uri BrokerUri
        {
            get
            {
                if(uri == null)
                {
                    uri = new Uri(AddressScheme + BrokerIpAddress + ":" + BrokerPort);
                }
                return uri;
            }
        }

        public string LogLevel { get => config?.Global?.LogLevel ?? DEFAULT_LOG_LEVEL; } 
        public bool AmqpFrameTrace { get => config?.Global?.FrameTrace ?? false; }
        public uint DataGeneratedChunkSize { get => config?.Global?.DataConfig == null ? DEFAULT_DATA_GENERATED_CHUNK_SIZE : config.Global.DataConfig.GenerateChunkSize; }

        public bool IsSecureBroker { get => String.Compare(this.AddressScheme, SECURE_BROKER_ADDRESS_SCHEME, true) == 0; }

        protected Configuration config = null;

        private TestConfig()
        {
            try
            {
                // Load config file path using test parameters.
                // Should the test parameters not be set load the 
                // config file with the default name in the current application directory.
                string configFilePath = NUnit.Framework.TestContext.Parameters.Get(Parameters.TS_PATH) ?? NUnit.Framework.TestContext.CurrentContext.TestDirectory;

                string configFileName = 
                    NUnit.Framework.TestContext.Parameters.Get(Parameters.TS_FILENAME) ?? Configuration.CONFIG_FILENAME;
                string filename = null;
                if (!String.IsNullOrEmpty(configFilePath))
                {
                    filename = string.Format("{0}{1}{2}", configFilePath, Path.DirectorySeparatorChar, configFileName);
                }
                else
                {
                    filename = configFileName;
                }
                this.config = ObjectXMLSerializer<Configuration>.Load(filename);
                
            }
            catch (Exception e)
            {
                Console.WriteLine("Failed to load unit test configuration. Cause : {0}.", e.Message);
                Console.Error.WriteLine(e);
            }
        }

        private static TestConfig inst = null;
        public static TestConfig Instance {
            get
            {
                if(inst == null)
                {
                    inst = new TestConfig();
                }
                return inst;
            }
        }

        public static class Parameters
        {
            public const string TS_PATH = "TestSuiteConfigurationPath";
            public const string TS_FILENAME = "TestSuiteConfigurationFileName";
        }
    }

    #region XML Configuration
    [Serializable]
    [XmlRoot]
    public class Configuration
    {
        public const string CONFIG_FILENAME = "TestSuite.config";

        [XmlElement(Type = typeof(Broker))]
        public Broker Broker = null;

        [XmlElement(Type = typeof(Global))]
        public Global Global = null;

    }

    [Serializable]
    public class Global
    {
        [XmlAttribute]
        public string LogLevel = null;
        [XmlAttribute]
        public bool FrameTrace = false;
        [XmlElement(Type = typeof(Data))]
        public Data DataConfig = null;
    }

    [Serializable]
    public class Data
    {
        [XmlAttribute]
        public uint GenerateChunkSize = uint.MaxValue;
    }

    [Serializable]
    public class Broker
    {
        [XmlAttribute]
        public string Scheme = null;
        [XmlAttribute]
        public string IPAddress = null;
        [XmlAttribute]
        public string Port = null;

        [XmlAttribute]
        public string Name = null;

        [XmlElement(Type = typeof(Client))]
        public Client Client = null;

    }

    public class Client
    {
        [XmlAttribute]
        public string Username = null;
        [XmlAttribute]
        public string Password = null;
        [XmlAttribute]
        public string ClientId = null;

        [XmlAttribute]
        public bool AcceptInvalidServerCert = true;
        [XmlAttribute]
        public string ClientCertFileName = null;
        [XmlAttribute]
        public string ClientCertSubject = null;
        [XmlAttribute]
        public string ClientCertPasword = null;
        [XmlAttribute]
        public string KeyStoreName = null;
        [XmlAttribute]
        public string KeyStoreLocation = null;
    }

    #endregion

    #region XML Object Serializer

    public static class ObjectXMLSerializer<T> where T : class
    {
        public static T Load(string path)
        {
            T serializableObject = LoadFromDocumentFormat(path);
            return serializableObject;
        }

        private static T LoadFromDocumentFormat (string path, Type[] extraTypes = null)
        {
            T SerializableObject = null;
            using(TextReader reader = CreateTextReader(path))
            {
                XmlSerializer serializer = CreateXmlSerializer(extraTypes);
                SerializableObject = serializer.Deserialize(reader) as T;
            }
            return SerializableObject;
        }

        private static TextReader CreateTextReader(string path)
        {
            return new StreamReader(path);
        }

        private static XmlSerializer CreateXmlSerializer(Type[] extraTypes)
        {
            Type objectType = typeof(T);
            XmlSerializer serializer = null;
            if(extraTypes != null)
            {
                serializer = new XmlSerializer(objectType, extraTypes);
            }
            else
            {
                serializer = new XmlSerializer(objectType);
            }
            return serializer;
        }
    }

    #endregion
}



