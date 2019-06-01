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

using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP
{
    public class NmsAcknowledgeCallback
    {
        private readonly NmsSession session;
        private readonly InboundMessageDispatch envelope;
        private AckType? ackType = null;

        public NmsAcknowledgeCallback(NmsSession session, InboundMessageDispatch envelope = null)
        {
            this.session = session;
            this.envelope = envelope;
        }

        public void Acknowledge()
        {
            if (session.IsClosed)
            {
                throw new IllegalStateException("Session closed.");
            }

            if (envelope == null)
            {
                session.Acknowledge(AcknowledgementType);
            }
            else
            {
                session.AcknowledgeIndividual(AcknowledgementType, envelope);
            }
        }

        public AckType AcknowledgementType
        {
            get => ackType ?? MessageSupport.DEFAULT_ACK_TYPE;
            set => ackType = value;
        }
    }
}