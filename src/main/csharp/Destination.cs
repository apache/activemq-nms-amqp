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
namespace Apache.NMS.Amqp
{

	/// <summary>
	/// Summary description for Destination.
	/// </summary>
	public abstract class Destination : IDestination
	{

		private String path = "";

		/**
		 * The Default Constructor
		 */
		protected Destination()
		{
		}

		/**
		 * Construct the Destination with a defined physical name;
		 *
		 * @param name
		 */
		protected Destination(String name)
		{
			Path = name;
		}

		public String Path
		{
			get { return this.path; }
			set
			{
				this.path = value;
                //if(!this.path.Contains("\\"))
                //{
                //    // Queues must have paths in them.  If no path specified, then
                //    // default to local machine.
                //    this.path = ".\\" + this.path;
                //}
			}
		}


		public bool IsTopic
		{
			get
			{
				return DestinationType == DestinationType.Topic
					|| DestinationType == DestinationType.TemporaryTopic;
			}
		}

		public bool IsQueue
		{
			get
			{
				return !IsTopic;
			}
		}


		public bool IsTemporary
		{
			get
			{
				return DestinationType == DestinationType.TemporaryQueue
					|| DestinationType == DestinationType.TemporaryTopic;
			}
		}

		/**
		 * @return string representation of this instance
		 */
		public override String ToString()
		{
			return this.path;
		}

		/**
		 * @return hashCode for this instance
		 */
		public override int GetHashCode()
		{
			int answer = 37;

			if(this.path != null)
			{
				answer = path.GetHashCode();
			}
			if(IsTopic)
			{
				answer ^= 0xfabfab;
			}
			return answer;
		}

		/**
		 * if the object passed in is equivalent, return true
		 *
		 * @param obj the object to compare
		 * @return true if this instance and obj are equivalent
		 */
		public override bool Equals(Object obj)
		{
			bool result = this == obj;
			if(!result && obj != null && obj is Destination)
			{
				Destination other = (Destination) obj;
				result = this.DestinationType == other.DestinationType
					&& this.path.Equals(other.path);
			}
			return result;
		}

		/**
		 * Factory method to create a child destination if this destination is a composite
		 * @param name
		 * @return the created Destination
		 */
		public abstract Destination CreateDestination(String name);


		public abstract DestinationType DestinationType
		{
			get;
		}

	}
}

