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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.NMS;
using System.Reflection;
using System.Diagnostics;

namespace Apache.NMS.AMQP
{
    /// <summary>
    /// Apache.NMS.AMQP.ConnectionMetaData implements Apache.NMS.IConnectionMetaData
    /// Contains version information for Apache.NMS and AmqpNetLite assemblies.
    /// </summary>
    class ConnectionMetaData : IConnectionMetaData
    {
        private static ConnectionMetaData inst = null;
        private static object StaticLock = new object();
        public static ConnectionMetaData Version
        {
            get
            {
                ConnectionMetaData instance = inst;
                // unsafe test for performance
                if (instance == null)
                {
                    lock (StaticLock)
                    {
                        // safe test 
                        instance = inst;
                        if (instance == null)
                        {
                            inst = new ConnectionMetaData();
                            instance = inst;
                        }
                        
                    }
                }
                return instance;
            }
        }

        private string AssemblyVersion = "-";
        private string NMSAssemblyVersion = "-";
        private string AMQPAssemblyVersion = "-";
        private string AssemblyFileVersion = "-";
        private string AssemblyInformationalVersion = "-";
        private string ProviderName = "-";
        private string AMQPAssemblyName = "-";
        private readonly int Major;
        private readonly int Minor;
        private readonly int NMSMajor;
        private readonly int NMSMinor;
        private ConnectionMetaData()
        {
            Assembly assembly = Assembly.GetAssembly(typeof(NmsConnectionFactory));
            AssemblyVersion = assembly.GetName().Version.ToString();

            ProviderName = assembly.GetName().Name;

            Assembly NMSAssembly = Assembly.GetAssembly(typeof(Apache.NMS.NMSConnectionFactory));
            NMSAssemblyVersion = NMSAssembly.GetName().Version.ToString();

            Assembly AMQPAssembly = Assembly.GetAssembly(typeof(Amqp.ConnectionFactory));
            AMQPAssemblyVersion = AMQPAssembly.GetName().Version.ToString();

            AMQPAssemblyName = AMQPAssembly.GetName().Name;

            try
            {
                FileVersionInfo info = FileVersionInfo.GetVersionInfo(assembly.Location);
                AssemblyFileVersion = info.FileVersion.ToString();
                AssemblyInformationalVersion = info.ProductVersion.ToString();

                string[] parts = AssemblyVersion.Split('.');
                if (parts.Length > 1)
                {
                    Major = Convert.ToInt32(parts[0]);
                    Minor = Convert.ToInt32(parts[1]);
                }
                else
                {
                    Major = -1;
                    Minor = -1;
                }

                parts = NMSAssemblyVersion.Split('.');
                if (parts.Length > 1)
                {
                    NMSMajor = Convert.ToInt32(parts[0]);
                    NMSMinor = Convert.ToInt32(parts[1]);
                }
                else
                {
                    NMSMajor = -1;
                    NMSMinor = -1;
                }


            }
            catch (Exception ex)
            {
                Tracer.ErrorFormat("Unable to load Provider version. Message: {0}", ex.Message);
            }
        }
        
        public int NMSMajorVersion
        {
            get
            {
                return NMSMajor;
            }
        }

        public int NMSMinorVersion
        {
            get
            {
                return NMSMinor;
            }
        }

        public string NMSProviderName
        {
            get
            {
                return ProviderName;
            }
        }

        public string NMSVersion
        {
            get
            {
                return string.Format("{0}.{1}.{2}",NMSMajorVersion, NMSMinorVersion, 2);
            }
        }

        public string[] NMSXPropertyNames
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public int ProviderMajorVersion
        {
            get
            {
                return Major;
            }
        }

        public int ProviderMinorVersion
        {
            get
            {
                return Minor;
            }
        }

        public string ProviderVersion
        {
            get
            {
                return AssemblyInformationalVersion;
            }
        }

        public override string ToString()
        {
            string result = "NMS AMQP Version: [\n";

            // NMS Version

            result += "NMSVersion = " + NMSMajorVersion + "." + NMSMinorVersion;

            // NMS Provider Assembly Information

            result += ",\nNMSProviderName = " + NMSProviderName;
            result += ",\nProvider AssemblyVersion = " + AssemblyVersion;
            result += ",\nProvider AssemblyFileVersion = " + AssemblyFileVersion;
            result += ",\nProvider AssemblyInformationalVersion = " + AssemblyInformationalVersion;

            // Amqp Library version Information

            result += ",\nProvider AMQP Assembly Name = " + AMQPAssemblyName;
            result += ",\nProvider AMQP Assembly Version = " + AMQPAssemblyVersion;

            result += "\n]";

            return result;
        }
    }
}
