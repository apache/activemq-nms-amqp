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

namespace NMS.AMQP
{
    /// <summary>
    /// NMS.AMQP.Queue implements Apache.NMS.IQueue
    /// Queue is an concrete implementation for an abstract Destination.
    /// </summary>
    class Queue : Destination, IQueue
    {
        
        #region Constructor

        internal Queue(Connection conn, string queueString) : base(conn, queueString, true)
        {}
        
        #endregion

        #region Destination Methods

        protected override void ValidateName(string name)
        {
            
        }

        #endregion

        #region Destination Properties

        public override DestinationType DestinationType
        {
            get
            {
                return DestinationType.Queue;
            }
        }

        #endregion

        #region IQueue Properties

        public string QueueName
        {
            get
            {
                return destinationName;
            }
        }

        #endregion

        #region IDisposable

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
        }

        #endregion
    }

    /// <summary>
    /// NMS.AMQP.TemporaryQueue implements Apache.NMS.ITemporaryQueue
    /// TemporaryQueue is an concrete implementation for an abstract TemporaryDestination.
    /// </summary>
    class TemporaryQueue : TemporaryDestination, ITemporaryQueue
    {
        #region Constructor

        internal TemporaryQueue(Connection conn) : base(conn, conn.TemporaryQueueGenerator.GenerateId(), true) { }

        internal TemporaryQueue(Connection conn, string destinationName) : base(conn, destinationName, true) { }
        
        #endregion

        #region Destination Methods

        protected override void ValidateName(string name)
        {
            
        }

        #endregion

        #region Destination Properties

        public override DestinationType DestinationType
        {
            get
            {
                return DestinationType.TemporaryQueue;
            }
        }

        #endregion

        #region IQueue Properties

        public string QueueName
        {
            get
            {
                return destinationName;
            }
        }

        #endregion

        #region ITemporaryQueue Methods

        public override void Delete()
        {
            base.Delete();
        }

        #endregion
    }

}
