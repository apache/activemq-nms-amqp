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
using NMS.AMQP.Message;
using Apache.NMS;
using Apache.NMS.Util;
using System.Threading;

namespace NMS.AMQP.Util.Types.Queue
{
    internal abstract class MessageQueueBase : NMSResource, IMessageQueue
    {
        protected const int INFINITE = -1;
        private object ThisLock = new object();
        private Atomic<bool> closed = new Atomic<bool>(false);
        protected MessageQueueBase() { }

        #region IMessageQueue abstract Properties

        public abstract int Count { get; }

        #endregion

        #region IMessageQueue Properties

        public bool IsEmpty { get { return Count == 0; } }

        public bool IsClosed { get { return closed.Value; } }

        public object SyncRoot { get { return ThisLock; } }

        public bool IsSynchronized { get { return true; } }

        #endregion

        #region IMessageQueue abstract Methods

        public abstract void Clear();
        
        public abstract IList<IMessageDelivery> RemoveAll();

        public abstract void Enqueue(IMessageDelivery message);

        public abstract void EnqueueFirst(IMessageDelivery message);

        public abstract void CopyTo(Array array, int index);

        #endregion

        #region abstract MessageQueueBase Methods

        protected abstract IMessageDelivery RemoveFirst();

        protected abstract IMessageDelivery PeekFirst();

        #endregion

        #region IMessageQueue Methods

        public IMessageDelivery Peek()
        {
            lock (SyncRoot)
            {
                return PeekFirst();
            }
        }

        public void Close()
        {
            if(closed.CompareAndSet(false, true))
            {
                lock (ThisLock)
                {
                    mode.GetAndSet(Resource.Mode.Stopped);

                }
            }
            
        }
        public IMessageDelivery Dequeue()
        {
            return Dequeue(INFINITE);
        }
        public IMessageDelivery Dequeue(int timeout)
        {
            TryDequeue(out IMessageDelivery value, timeout);
            return value;
        }
        public IMessageDelivery DequeueNoWait()
        {
            lock (SyncRoot)
            {
                if(IsClosed || !IsStarted || IsEmpty)
                {
                    return null;
                }
                return RemoveFirst();
            }
        }
        public virtual IEnumerator GetEnumerator()
        {
            IMessageDelivery[] messages = new IMessageDelivery[Count];
            this.CopyTo(messages, 0);
            return new MessageQueueEnumerator(messages);
        }

        #endregion

        #region Protected Methods

        protected bool TryDequeue(out IMessageDelivery value, int timeout = -1)
        {
            value = null;
            lock (SyncRoot)
            {
                bool signaled = true;
                while (IsEmpty)
                {
                    if (IsClosed || mode.Value.Equals(Resource.Mode.Stopping))
                    {
                        return false;
                    }
                    signaled = (timeout > -1) ? Monitor.Wait(SyncRoot, timeout) : Monitor.Wait(SyncRoot);
                    if (!signaled && timeout > -1)
                    {
                        return false;
                    }
                }
                if (!signaled)
                {
                    return false;
                }
                value = RemoveFirst();

            }
            return value != null;
        }

        #endregion

        #region NMSResource Methods

        protected override void ThrowIfClosed()
        {
            if (IsClosed)
            {
                throw new NMSException("Message Queue closed.");
            }
        }

        protected override void StopResource()
        {
            lock (SyncRoot)
            {
                Monitor.PulseAll(SyncRoot);
            }
        }

        protected override void StartResource()
        {
            lock (SyncRoot)
            {
                mode.GetAndSet(Resource.Mode.Started);
                Monitor.PulseAll(SyncRoot);
            }
        }

        #endregion

        #region Enumerator Class

        protected class MessageQueueEnumerator: IEnumerator
        {
            protected IEnumerator enumerator; 
            internal MessageQueueEnumerator(Array array)
            {
                enumerator = array.GetEnumerator();
            }

            public object Current { get { return enumerator.Current; } }
            
            public bool MoveNext()
            {
                return enumerator.MoveNext();
            }

            public void Reset()
            {
                enumerator.Reset();
            }
        }

        #endregion
    }

    internal class MessageDelivery : IMessageDelivery
    {

        internal MessageDelivery()
        {
        }

        internal MessageDelivery(Message.Message m)
        {
            Message = m;
        }

        public Message.Message Message { get; internal set; } = null;

        public MsgPriority Priority
        {
            get
            {
                return Message.NMSPriority;
            }
        }

        public int DeliveryCount
        {
            get
            {
                return Message.GetMessageCloak().DeliveryCount;
            }
        }

        public bool EnqueueFirst { get; internal set; } = false;

        public override string ToString()
        {
            return string.Format("[Message:{0}, First={1}, Priority={2}]", this.Message, this.EnqueueFirst, Priority);
        }

    }

}
