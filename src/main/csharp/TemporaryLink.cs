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
using Apache.NMS;
using NMS.AMQP.Util;
using Amqp;
using Amqp.Framing;

namespace NMS.AMQP
{
    internal class TemporaryLink : MessageLink
    {
        private readonly string TOPIC = "Topic";
        private readonly string QUEUE = "Queue";
        private readonly string CREATOR_TOPIC = "temp-topic-creator";
        private readonly string CREATOR_QUEUE = "temp-queue-creator";

        internal TemporaryLink(Session session, TemporaryDestination destination) : base(session, destination)
        {
            Info = new TemporaryLinkInfo(destination.DestinationId);
            this.RequestTimeout = session.Connection.RequestTimeout;
        }

        private TemporaryDestination TemporaryDestination { get => Destination as TemporaryDestination; }

        private bool IsTopic { get => TemporaryDestination.IsTopic; }

        private string LocalDestinationName { get => TemporaryDestination.DestinationId.ToString(); }

        private string DestinationTypeName { get { return IsTopic ? TOPIC : QUEUE; } }

        private void OnAttachResponse(ILink link, Attach attachResponse)
        {
            Tracer.InfoFormat("Received attach response for Temporary creator link. Link = {0}, Attach = {1}", link.Name, attachResponse);
            Target target = (attachResponse.Target as Amqp.Framing.Target);
            if(target?.Address != null)
            {
                this.TemporaryDestination.DestinationName = target.Address;
            }
            this.OnResponse();
        }

        private Source CreateSource()
        {
            Source result = new Source();

            return result;
        }

        private Target CreateTarget()
        {
            Target result = new Target();
            result.Durable = (uint)TerminusDurability.NONE;

            result.Capabilities = new[] { SymbolUtil.GetTerminusCapabilitiesForDestination(Destination) };
            result.Dynamic = true;

            result.ExpiryPolicy = SymbolUtil.ATTACH_EXPIRY_POLICY_LINK_DETACH;
            Amqp.Types.Fields dnp = new Amqp.Types.Fields();
            dnp.Add(
                SymbolUtil.ATTACH_DYNAMIC_NODE_PROPERTY_LIFETIME_POLICY,
                SymbolUtil.DELETE_ON_CLOSE
                );
            result.DynamicNodeProperties = dnp;

            return result;
        }

        private Attach CreateAttach()
        {
            Attach result = new Attach()
            {
                Source = CreateSource(),
                Target = CreateTarget(),
                SndSettleMode = SenderSettleMode.Unsettled,
                RcvSettleMode = ReceiverSettleMode.First,
            };

            return result;
        }

        protected override ILink CreateLink()
        {
            Amqp.Session parentImpl = this.Session.InnerSession as Amqp.Session;
            string linkDestinationName = "apache-nms:" + ((IsTopic) ? CREATOR_TOPIC : CREATOR_QUEUE ) + LocalDestinationName;
            SenderLink link = new SenderLink(parentImpl, linkDestinationName, CreateAttach(), OnAttachResponse);
            return link;
        }

        protected override void OnInternalClosed(IAmqpObject sender, Error error)
        {
            base.OnInternalClosed(sender, error);
            this.OnResponse();
        }

        internal override void Shutdown()
        {
            base.Shutdown();
            this.Session.Connection.Remove(this.TemporaryDestination);
        }

        protected override void StopResource()
        {
        }

    }

    internal class TemporaryLinkInfo : LinkInfo
    {
        public TemporaryLinkInfo (Id id) : base(id)
        {

        }
        
    }
}
