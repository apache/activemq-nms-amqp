using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Apache.NMS.AMQP.Meta;

namespace Apache.NMS.AMQP.Provider.Amqp
{
    public class AmqpSubscriptionTracker
    {

        // Subscription Name Delimiter
        public readonly static string SUB_NAME_DELIMITER = "|";

        private readonly ISet<string> exclusiveDurableSubs = new HashSet<string>();

        private readonly IDictionary<string, SubDetails> sharedDurableSubs =
            new ConcurrentDictionary<string, SubDetails>();

        private readonly IDictionary<string, SubDetails> sharedVolatileSubs =
            new ConcurrentDictionary<string, SubDetails>();

        public string ReserveNextSubscriptionLinkName(string subscriptionName, NmsConsumerInfo consumerInfo)
        {
            ValidateSubscriptionName(subscriptionName);

            if (consumerInfo == null)
            {
                throw new ArgumentException("Consumer info must not be null.");
            }

            if (consumerInfo.IsShared)
            {
                if (consumerInfo.IsDurable)
                {
                    return GetSharedDurableSubLinkName(subscriptionName, consumerInfo);
                }
                else
                {
                    return GetSharedVolatileSubLinkName(subscriptionName, consumerInfo);
                }
            }
            else if (consumerInfo.IsDurable)
            {
                RegisterExclusiveDurableSub(subscriptionName);
                return subscriptionName;
            }
            else
            {
                throw new IllegalStateException(
                    "Non-shared non-durable sub link naming is not handled by the tracker.");
            }
        }

        private void ValidateSubscriptionName(string subscriptionName)
        {
            if (string.IsNullOrEmpty(subscriptionName))
            {
                throw new ArgumentException("Subscription name must not be null or empty.");
            }

            if (subscriptionName.Contains(SUB_NAME_DELIMITER))
            {
                throw new ArgumentException(
                    "Subscription name must not contain '" + SUB_NAME_DELIMITER + "' character.");
            }
        }

        private string GetSharedDurableSubLinkName(string subscriptionName, NmsConsumerInfo consumerInfo)
        {
            IDestination topic = consumerInfo.Destination;
            string selector = consumerInfo.Selector;

            SubDetails subDetails = null;
            if (sharedDurableSubs.ContainsKey(subscriptionName))
            {
                subDetails = sharedDurableSubs[subscriptionName];

                if (subDetails.Matches(topic, selector))
                {
                    subDetails.AddSubscriber(consumerInfo);
                }
                else
                {
                    throw new NMSException("Subscription details dont match existing subscriber.");
                }
            }
            else
            {
                subDetails = new SubDetails(topic, selector, consumerInfo);
            }

            sharedDurableSubs.Add(subscriptionName, subDetails);

            int count = subDetails.TotalSubscriberCount();

            return GetDurableSubscriptionLinkName(subscriptionName, consumerInfo.IsExplicitClientId, count);
        }

        private string GetDurableSubscriptionLinkName(string subscriptionName, bool hasClientID, int count)
        {
            string linkName = GetFirstDurableSubscriptionLinkName(subscriptionName, hasClientID);
            if (count > 1)
            {
                if (hasClientID)
                {
                    linkName += SUB_NAME_DELIMITER + count;
                }
                else
                {
                    linkName += count;
                }
            }

            return linkName;
        }

        public string GetFirstDurableSubscriptionLinkName(string subscriptionName, bool hasClientID)
        {
            ValidateSubscriptionName(subscriptionName);

            String receiverLinkName = subscriptionName;
            if (!hasClientID)
            {
                receiverLinkName += SUB_NAME_DELIMITER + "global";
            }

            return receiverLinkName;
        }

        private String GetSharedVolatileSubLinkName(string subscriptionName, NmsConsumerInfo consumerInfo)
        {
            IDestination topic = consumerInfo.Destination;
            string selector = consumerInfo.Selector;

            SubDetails subDetails = null;
            if (sharedVolatileSubs.ContainsKey(subscriptionName))
            {
                subDetails = sharedVolatileSubs[subscriptionName];

                if (subDetails.Matches(topic, selector))
                {
                    subDetails.AddSubscriber(consumerInfo);
                }
                else
                {
                    throw new NMSException("Subscription details dont match existing subscriber");
                }
            }
            else
            {
                subDetails = new SubDetails(topic, selector, consumerInfo);
            }

            sharedVolatileSubs.Add(subscriptionName, subDetails);

            string receiverLinkName = subscriptionName + SUB_NAME_DELIMITER;
            int count = subDetails.TotalSubscriberCount();

            if (consumerInfo.IsExplicitClientId)
            {
                receiverLinkName += "volatile" + count;
            }
            else
            {
                receiverLinkName += "global-volatile" + count;
            }

            return receiverLinkName;
        }

        private void RegisterExclusiveDurableSub(String subscriptionName)
        {
            exclusiveDurableSubs.Add(subscriptionName);
        }

        /**
         * Checks if there is an exclusive durable subscription already
         * recorded as active with the given subscription name.
         *
         * @param subscriptionName name of subscription to check
         * @return true if there is an exclusive durable sub with this name already active
         */
        public bool IsActiveExclusiveDurableSub(String subscriptionName)
        {
            return exclusiveDurableSubs.Contains(subscriptionName);
        }

        /**
         * Checks if there is a shared durable subscription already
         * recorded as active with the given subscription name.
         *
         * @param subscriptionName name of subscription to check
         * @return true if there is a shared durable sub with this name already active
         */
        public bool IsActiveSharedDurableSub(string subscriptionName)
        {
            return sharedDurableSubs.ContainsKey(subscriptionName);
        }

        /**
         * Checks if there is either a shared or exclusive durable subscription
         * already recorded as active with the given subscription name.
         *
         * @param subscriptionName name of subscription to check
         * @return true if there is a durable sub with this name already active
         */
        public bool IsActiveDurableSub(string subscriptionName)
        {
            return IsActiveExclusiveDurableSub(subscriptionName) || IsActiveSharedDurableSub(subscriptionName);
        }

        /**
         * Checks if there is an shared volatile subscription already
         * recorded as active with the given subscription name.
         *
         * @param subscriptionName name of subscription to check
         * @return true if there is a shared volatile sub with this name already active
         */
        public bool IsActiveSharedVolatileSub(String subscriptionName)
        {
            return sharedVolatileSubs.ContainsKey(subscriptionName);
        }

        public void ConsumerRemoved(NmsConsumerInfo consumerInfo)
        {
            string subscriptionName = consumerInfo.SubscriptionName;

            if (!string.IsNullOrEmpty(subscriptionName))
            {
                if (consumerInfo.IsShared)
                {
                    if (consumerInfo.IsDurable)
                    {
                        if (sharedDurableSubs.ContainsKey(subscriptionName))
                        {
                            SubDetails subDetails = sharedDurableSubs[subscriptionName];
                            subDetails.RemoveSubscriber(consumerInfo);

                            int count = subDetails.ActiveSubscribers();
                            if (count < 1)
                            {
                                sharedDurableSubs.Remove(subscriptionName);
                            }
                        }
                    }
                    else
                    {
                        if (sharedVolatileSubs.ContainsKey(subscriptionName))
                        {
                            SubDetails subDetails = sharedVolatileSubs[subscriptionName];
                            subDetails.RemoveSubscriber(consumerInfo);

                            int count = subDetails.ActiveSubscribers();
                            if (count < 1)
                            {
                                sharedVolatileSubs.Remove(subscriptionName);
                            }
                        }
                    }
                }
                else if (consumerInfo.IsDurable)
                {
                    exclusiveDurableSubs.Remove(subscriptionName);
                }
            }
        }

        private class SubDetails
        {
            private IDestination topic = null;
            private String selector = null;
            private ISet<NmsConsumerInfo> subscribers = new HashSet<NmsConsumerInfo>();
            private int totalSubscriberCount;

            public SubDetails(IDestination topic, string selector, NmsConsumerInfo info)
            {
                this.topic = topic ?? throw new ArgumentException("Topic destination must not be null");
                this.selector = selector;
                AddSubscriber(info);
            }

            public void AddSubscriber(NmsConsumerInfo info)
            {
                if (info == null)
                {
                    throw new ArgumentException("Consumer info must not be null");
                }

                totalSubscriberCount++;
                subscribers.Add(info);
            }

            public void RemoveSubscriber(NmsConsumerInfo info)
            {
                subscribers.Remove(info);
            }

            public int ActiveSubscribers()
            {
                return subscribers.Count;
            }

            public int TotalSubscriberCount()
            {
                return totalSubscriberCount;
            }

            public bool Matches(IDestination newTopic, string newSelector)
            {
                if (!topic.Equals(newTopic))
                {
                    return false;
                }

                if (selector == null)
                {
                    return newSelector == null;
                }
                else
                {
                    return selector.Equals(newSelector);
                }
            }

        }
    }
}