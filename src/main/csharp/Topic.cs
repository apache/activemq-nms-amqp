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
    /// NMS.AMQP.Topic implements Apache.NMS.ITopic
    /// Topic is an concrete implementation for an abstract Destination.
    /// </summary>
    class Topic : Destination, ITopic
    {
        
        #region Constructor

        internal Topic(Connection conn, string topicString) : base(conn, topicString, false) {}

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
                return DestinationType.Topic;
            }
        }

        #endregion

        #region ITopic Properties

        public string TopicName
        {
            get
            {
                return destinationName;
            }
        }

        #endregion

        #region IDisposable Methods

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
        }

        #endregion
    }

    /// <summary>
    /// NMS.AMQP.TemporaryTopic implements Apache.NMS.ITemporaryTopic.
    /// TemporaryTopic is an concrete implementation for an abstract TemporaryDestination.
    /// </summary>
    class TemporaryTopic :  TemporaryDestination, ITemporaryTopic
    {
        #region Constructor

        internal TemporaryTopic(Connection conn) : base(conn, conn.TemporaryTopicGenerator.GenerateId(), false) { }

        internal TemporaryTopic(Connection conn, string destinationName) : base(conn, destinationName, false) { }

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
                return DestinationType.TemporaryTopic;
            }
        }

        #endregion

        #region ITopic Properties

        public string TopicName
        {
            get
            {
                return destinationName;
            }
        }

        #endregion

        #region TemporaryDestination Methods

        public override void Delete()
        {
            base.Delete();
        }

        #endregion 
    }
}
