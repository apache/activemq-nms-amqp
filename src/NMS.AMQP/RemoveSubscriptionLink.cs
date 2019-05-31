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
using Apache.NMS;
using Apache.NMS.AMQP.Util;
using Amqp;
using Amqp.Framing;
using System;

namespace Apache.NMS.AMQP
{
    /// <summary>
    /// RemoveSubscriptionLink handles the amqp consumer link attach frames for the durable 
    /// consumer unsubscribe operation.
    /// </summary>
    internal class RemoveSubscriptionLink : MessageLink
    {
        private Amqp.Framing.Source remoteSource = null;

        internal RemoveSubscriptionLink(Session session, string name) : base(session, null)
        {
            IdGenerator idgen = new CustomIdGenerator(true, session.Connection.ClientId, name);
            Info = new SubscriptionInfo(idgen.GenerateId())
            {
                SubscriptionName = name,
                ClientId = session.Connection.ClientId
            };
            Configure();
        }

        protected new SubscriptionInfo Info
        {
            get
            {
                return base.Info as SubscriptionInfo;
            }
            set
            {
                base.Info = value;
            }
        }

        #region Public Methods

        /// <summary>
        /// Removes the remote durable subscription. Throws <see cref="InvalidDestinationException"/> when subscription name is invalid or does not exist.
        /// </summary>
        /// <exception cref="InvalidDestinationException"/>
        public void Unsubscribe()
        {
            ThrowIfClosed();
            // attach will throw an exception on non existent subscriptions.
            this.Attach();
            // send amqp detach with closed set to true.
            this.Close();
        }

        #endregion

        #region MessageLink Methods

        protected void OnAttachResponse(ILink sender, Attach attachResponse)
        {
            Tracer.InfoFormat("Attempting to close subscription {0}. Attach response {1}", this.Info.SubscriptionName, attachResponse);
            this.remoteSource = attachResponse.Source as Amqp.Framing.Source;
            if (this.remoteSource != null)
            {
                Tracer.InfoFormat("Found subscription {0} on remote with source {1}.", this.Info.SubscriptionName, this.remoteSource);
                this.OnResponse();
            }
            
        }

        protected override void OnFailure()
        {
            string failureMessage = "";
            if (String.Compare(this.Link.Error?.Condition, Amqp.ErrorCode.NotFound, false) == 0)
            {
                failureMessage = string.Format(
                        "Cannot remove Subscription {0} that does not exists",
                        this.Info.SubscriptionName
                        );
            }
            else
            {
                failureMessage = string.Format("Subscription {0} unsubscribe operation failure", this.Info.SubscriptionName);
            }
            throw ExceptionSupport.GetException(this.Link, failureMessage);
        }

        protected override ILink CreateLink()
        {
            Attach attach = new Attach()
            {
                LinkName = this.Info.SubscriptionName
            };

            ILink removeLink = new ReceiverLink(
                this.Session.InnerSession as Amqp.Session,
                this.Info.SubscriptionName,
                attach, 
                this.OnAttachResponse);
            return removeLink;
        }

        protected override void OnInternalClosed(IAmqpObject sender, Error error)
        {
            Tracer.InfoFormat("Closed subscription {0} state {1}", this.Info.SubscriptionName, this.State);
            if (error != null && !this.IsOpening && !this.IsClosing)
            {
                if (this.remoteSource == null)
                {
                    // ignore detach request
                    if (error != null)
                    {
                        Tracer.DebugFormat("Received detach request on invalid subscription {0}. Cause for detach : {1}", this.Info.SubscriptionName, error);
                    }
                }
                else
                {
                    Tracer.WarnFormat("Subscription {0} on connection {1} has been destroyed. Cause {2}", this.Info.SubscriptionName, this.Info.ClientId, error);
                }
            }
            base.OnInternalClosed(sender, error);
            this.OnResponse();
        }

        protected override void StartResource()
        {
            // Remove Subscription Link does not start or stop it uses unsubscribe instead.
            throw new System.NotImplementedException();
        }

        protected override void StopResource()
        {
            // Remove Subscription Link does not start or stop it uses unsubscribe instead.
            throw new System.NotImplementedException();
        }

        #endregion

    }

    internal class SubscriptionInfo : LinkInfo
    {
        internal SubscriptionInfo(Id id) : base (id)
        {
        }
        public string SubscriptionName { get; internal set; }
        public string ClientId { get; internal set; }
    }
}
