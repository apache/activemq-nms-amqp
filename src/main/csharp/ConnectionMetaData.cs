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
using System.Reflection;

namespace Apache.NMS.Amqp
{
	/// <summary>
	/// Implements the Connection Meta-Data feature for Apache.NMS.Qpid/Amqp
	/// </summary>
	public class ConnectionMetaData : IConnectionMetaData
	{
		private int nmsMajorVersion;
		private int nmsMinorVersion;

		private string nmsProviderName;
		private string nmsVersion;

		private int providerMajorVersion;
		private int providerMinorVersion;
		private string providerVersion;

		private string[] nmsxProperties;

		public ConnectionMetaData()
		{
			Assembly self = Assembly.GetExecutingAssembly();
			AssemblyName asmName = self.GetName();

			this.nmsProviderName = asmName.Name;
			this.providerMajorVersion = asmName.Version.Major;
			this.providerMinorVersion = asmName.Version.Minor;
			this.providerVersion = asmName.Version.ToString();

			this.nmsxProperties = new String[] { };

			foreach(AssemblyName name in self.GetReferencedAssemblies())
			{
				if(0 == string.Compare(name.Name, "Apache.NMS", true))
				{
					this.nmsMajorVersion = name.Version.Major;
					this.nmsMinorVersion = name.Version.Minor;
					this.nmsVersion = name.Version.ToString();

					return;
				}
			}

			throw new NMSException("Could not find a reference to the Apache.NMS Assembly.");
		}

		public int NMSMajorVersion
		{
			get { return this.nmsMajorVersion; }
		}

		public int NMSMinorVersion
		{
			get { return this.nmsMinorVersion; }
		}

		public string NMSProviderName
		{
			get { return this.nmsProviderName; }
		}

		public string NMSVersion
		{
			get { return this.nmsVersion; }
		}

		public string[] NMSXPropertyNames
		{
			get { return this.nmsxProperties; }
		}

		public int ProviderMajorVersion
		{
			get { return this.providerMajorVersion; }
		}

		public int ProviderMinorVersion
		{
			get { return this.providerMinorVersion; }
		}

		public string ProviderVersion
		{
			get { return this.providerVersion; }
		}
	}
}
