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
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    public class AmqpTemporaryDestination
    {
        private const string CREATOR_TOPIC = "temp-topic-creator";
        private const string CREATOR_QUEUE = "temp-queue-creator";
        
        private readonly AmqpSession session;
        private readonly NmsTemporaryDestination destination;
        private SenderLink senderLink;

        public AmqpTemporaryDestination(AmqpSession session, NmsTemporaryDestination destination)
        {
            this.session = session;
            this.destination = destination;
        }

        public Task Attach()
        {
            Attach result = new Attach
            {
                Source = CreateSource(),
                Target = CreateTarget(),
                SndSettleMode = SenderSettleMode.Unsettled,
                RcvSettleMode = ReceiverSettleMode.First,
            };

            string linkDestinationName = "apache-nms:" + ((destination.IsTopic) ? CREATOR_TOPIC : CREATOR_QUEUE) + destination.Address;
            var taskCompletionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            senderLink = new SenderLink(session.UnderlyingSession, linkDestinationName, result, (link, attach) =>
            {
                // Once our sender is opened we can read the updated address from the target address.
                if (attach.Target is Target target && target.Address != null)
                {
                    string oldDestinationAddress = destination.Address;
                    string destinationAddress = target.Address;
                    destination.Address = target.Address;
                    
                    Tracer.Debug($"Updated temp destination to: {destinationAddress} from: {oldDestinationAddress}");
                }

                taskCompletionSource.SetResult(true);
            });

            senderLink.AddClosedCallback((sender, error) =>
            {
                NMSException exception = ExceptionSupport.GetException(error, $"Received attach response for Temporary creator link. Link = {destination}");
                taskCompletionSource.TrySetException(exception);
            });
            return taskCompletionSource.Task;
        }

        private Source CreateSource() => new Source();

        private Target CreateTarget()
        {
            Target result = new Target();
            result.Durable = (uint) TerminusDurability.NONE;

            result.Capabilities = new[] { SymbolUtil.GetTerminusCapabilitiesForDestination(destination) };
            result.Dynamic = true;

            result.ExpiryPolicy = SymbolUtil.ATTACH_EXPIRY_POLICY_LINK_DETACH;
            Fields dnp = new Fields();
            dnp.Add(
                SymbolUtil.ATTACH_DYNAMIC_NODE_PROPERTY_LIFETIME_POLICY,
                SymbolUtil.DELETE_ON_CLOSE
            );
            result.DynamicNodeProperties = dnp;

            return result;
        }

        public void Close()
        {
            try
            {
                senderLink.Close();
            }
            catch (NMSException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw ExceptionSupport.Wrap(ex, "Failed to close Link {0}", destination);
            }
        }
    }
}