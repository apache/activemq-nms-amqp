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
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP
{
    #region Destination Implementation
    /// <summary>
    /// Apache.NMS.AMQP.Destination implements Apache.NMS.IDestination
    /// Destionation is an abstract container for a Queue or Topic.
    /// </summary>
    abstract class Destination : IDestination
    {

        protected string destinationName;
        protected Connection connection;
        private readonly bool queue;

        #region Constructor

        internal Destination(Connection conn, string name, bool isQ)
        {
            queue = isQ;
            ValidateName(name);
            destinationName = name;
            connection = conn;
            
        }

        internal Destination(Destination other)
        {
            this.queue = other.queue;
            destinationName = other.destinationName;
            connection = other.connection;
        }

        #endregion

        #region Abstract Methods

        protected abstract void ValidateName(string name);

        #endregion

        #region IDestination Properties

        public virtual DestinationType DestinationType
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public virtual bool IsQueue
        {
            get
            {
                return queue;
            }
        }

        public virtual bool IsTemporary
        {
            get
            {
                return false;
            }
        }

        public virtual bool IsTopic
        {
            get
            {
                return !queue;
            }
        }

        #endregion

        #region IDisposable Methods

        public void Dispose()
        {
            try
            {
                this.Dispose(true);
            }
            catch (Exception ex)
            {
                Tracer.DebugFormat("Caught Exception while disposing of {0} : {1}. Exception : {2}",
                    this.DestinationType, this.destinationName, ex);
            }
        }

        protected virtual void Dispose(bool disposing)
        {

        }

        #endregion

        #region Object Comparison Methods

        public override string ToString()
        {
            return base.ToString() + ":" + destinationName;
        }

        public virtual bool Equals (Destination other)
        {
            return this.DestinationType == other.DestinationType && this.destinationName.Equals(other.destinationName);
        }

        public virtual bool Equals (IDestination destination)
        {
            if (this.DestinationType == destination.DestinationType)
            {
                if (destination is Destination)
                {
                    return this.Equals(destination as Destination);
                }
                else
                {
                    string destName = destination.IsTopic ? (destination as ITopic).TopicName : (destination as IQueue).QueueName;
                    return (destName != null && destName.Length > 0) ? destName.CompareTo(this.destinationName) == 0 : false;
                }
            }
            return false;
        }

        public override bool Equals(object obj)
        {
            if (obj != null && obj is IDestination)
            {
                return this.Equals(obj as IDestination);
            }
            return false;
        }

        public override int GetHashCode()
        {
            return destinationName.GetHashCode() * 31 + DestinationType.GetHashCode();
        }

        #endregion
    }

    #endregion

    #region Temporary Destination Implementation

    /// <summary>
    /// Apache.NMS.AMQP.TemporaryDestination inherits NMS.AMQP.Destination
    /// Destionation is an abstract container for a Temporary Queue or Temporary Topic.
    /// </summary>
    abstract class TemporaryDestination : Destination
    {
        
        private readonly Id destinationId;

        private bool deleted = false;

        #region Constructor

        public TemporaryDestination(Connection conn, Id name, bool isQ) : base(conn, name.ToString(), isQ)
        {
            destinationId = name;
        }

        public TemporaryDestination(Connection conn, string name, bool isQ) : base(conn, name, isQ)
        {
            destinationId = new Id(name);
        }

        #endregion

        #region Temporary Destination Properties

        internal Connection Connection
        {
            get { return connection; }
        }

        internal Id DestinationId
        {
            get
            {
                return destinationId;
            }
        }
        
        internal bool IsDeleted { get => deleted; }

        internal string DestinationName
        {
            get => base.destinationName;
            set => base.destinationName = value;
        }

        #endregion

        #region Temporary Destination Methods

        public virtual void Delete()
        {
            if (connection != null)
            {
                this.connection.DestroyTemporaryDestination(this);
                connection = null;
            }
            deleted = true;
        }

        #endregion

        #region IDestination Methods

        public override bool IsTemporary
        {
            get
            {
                return true;
            }
        }

        #endregion

        #region IDisposable Methods

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.Delete();
            }
        }
        
        #endregion

        #region Object Comparison Methods

        public override int GetHashCode()
        {
            return destinationId.GetHashCode();
        }

        public override bool Equals(Destination other)
        {
            if(other is TemporaryDestination)
            {
                return (other as TemporaryDestination).destinationId.Equals(this.destinationId) 
                    || base.Equals(other);
            }
            return base.Equals(other);
        }

        #endregion

    }

    #endregion

    #region Destination Transformation

    internal class DestinationTransformation
    {
        public static Destination Transform(Connection connection, IDestination destination)
        {
            Destination transformDestination = null;
            if (destination == null)
                return null;

            if (destination is Destination)
            {
                return destination as Destination;
            }
            string destinationName = null;

            DestinationType type = destination.DestinationType;
            switch (type)
            {
                case DestinationType.Queue:
                case DestinationType.TemporaryQueue:
                    destinationName = (destination as IQueue).QueueName;
                    break;
                case DestinationType.Topic:
                case DestinationType.TemporaryTopic:
                    destinationName = (destination as ITopic).TopicName;
                    break;
                default:
                    throw new NMSException(string.Format("Unresolved destination. Unrecognized destination Type {0} for IDesintation {1}", type, destination?.ToString()));
            }

            if(destinationName == null)
            {
                throw new NMSException(string.Format("Unresolved destination. Could not resolved destination name for destination {0} type {1}.", destination?.ToString(), type));
            }

            switch (type)
            {
                case DestinationType.Queue:
                    transformDestination = new Queue(connection, destinationName);
                    break;
                case DestinationType.TemporaryQueue:
                    transformDestination = new TemporaryQueue(connection, destinationName);
                    break;
                case DestinationType.Topic:
                    transformDestination = new Topic(connection, destinationName);
                    break;
                case DestinationType.TemporaryTopic:
                    transformDestination = new TemporaryTopic(connection, destinationName);
                    break;
            }

            return transformDestination;
        }
    }

    #endregion
}
