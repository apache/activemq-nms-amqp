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
using System.Collections;
using System.Collections.Generic;
using Apache.NMS;

namespace NMS.AMQP.Util
{
    
    #region Abstract Link Cache

    internal abstract class LinkCache<TKey, TLink> : ICollection, IDisposable where TKey : class where TLink : MessageLink
    {
        protected readonly IDictionary<TKey, TLink> cachedLinks;

        public LinkCache()
        {
            cachedLinks = CreateCache();
        }

        #region ICollection Properties

        public int Count { get => cachedLinks.Count; }

        public object SyncRoot { get => cachedLinks; }

        public bool IsSynchronized { get => false; }

        #endregion

        #region ICollection Methods

        public abstract void CopyTo(Array array, int index);
        
        public IEnumerator GetEnumerator()
        {
            return (cachedLinks as IEnumerable).GetEnumerator();
        }

        #endregion

        #region Link Cache Properties

        internal ICollection<TKey> Keys { get => cachedLinks.Keys; }

        internal ICollection<TLink> Links { get => cachedLinks.Values; }

        #endregion

        #region Link Cache Methods

        public TLink GetLink(TKey cacheId)
        {
            TLink link = null;
            if (!IsValidKey(cacheId)) return link;
            if(!cachedLinks.TryGetValue(cacheId, out link))
            {
                Tracer.InfoFormat("Failed to get Link for cache request id {0}", cacheId);
            }
            return link;
        }

        public void AddLink(TKey cacheId, TLink link)
        {
            if (HasLink(cacheId))
            {
                throw new NMSException("Cannot add link to cache with Id {0} which is already in use.");
            }
            cachedLinks.Add(cacheId, link);
        }

        public TLink RemoveLink(TKey cacheId)
        {
            TLink linkToRemove = null;
            cachedLinks.TryGetValue(cacheId, out linkToRemove);
            cachedLinks.Remove(cacheId);
            return linkToRemove;
        }

        public bool HasLink(TKey cacheId)
        {
            return IsValidKey(cacheId) && cachedLinks.ContainsKey(cacheId);
        }

        public virtual void Close() { }

        public virtual void Dispose()
        {
            Close();
        }

        #endregion

        #region Abstract Link Cache Methods

        protected abstract IDictionary<TKey, TLink> CreateCache();
        protected abstract bool IsValidKey(TKey cacheId);
        
        #endregion
    }

    #endregion

    #region Temporary Link Cache 

    internal class TemporaryLinkCache : LinkCache<TemporaryDestination, TemporaryLink>
    {
        private Connection connection;
        private Session tempSession;
        internal TemporaryLinkCache(Connection connection) : base()
        {
            this.connection = connection;

        }

        internal Session Session
        {
            get
            {
                if(tempSession == null)
                {
                    tempSession = connection.CreateSession() as Session;
                }
                return tempSession;
            }
        }

        protected override bool IsValidKey(TemporaryDestination cacheId)
        {
            return cacheId != null && cacheId.DestinationId != null && cacheId.DestinationId.Size > 0;
        }

        protected override IDictionary<TemporaryDestination, TemporaryLink> CreateCache()
        {
            return new Dictionary<TemporaryDestination, TemporaryLink>(new TemporaryDestinationComparer());
        }

        protected sealed class TemporaryDestinationComparer : IEqualityComparer<TemporaryDestination>
        {
            public bool Equals(TemporaryDestination x, TemporaryDestination y)
            {
                return x.Equals(y);
            }

            public int GetHashCode(TemporaryDestination obj)
            {
                return obj.GetHashCode();
            }
        }

        public override void CopyTo(Array array, int index)
        {
            (this.cachedLinks as Dictionary<TemporaryDestination, TemporaryLink> as IDictionary).CopyTo(array, index);
        }

        public override void Close()
        {
            base.Close();
            this.cachedLinks.Clear();
            if(tempSession != null)
            {
                tempSession.Close();
                tempSession = null;
            }
        }
    }

    #endregion
    
    #region Anonymous Link Cache

    //TODO implement for Anonymous Fallback producer.

    #endregion
}
