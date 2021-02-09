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

using System.Collections;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP
{
    public class NmsQueueBrowser : IQueueBrowser, IEnumerator
    {
        private readonly object syncRoot = new object();

        private readonly NmsSession session;
        private readonly IQueue destination;
        private readonly string selector;

        private volatile NmsMessageConsumer consumer;

        private IMessage current;
        private readonly AtomicBool closed = new AtomicBool();
        
        public NmsQueueBrowser(NmsSession session, IQueue destination, string selector)
        {
            this.session = session;
            this.destination = destination;
            this.selector = selector;
        }

        public IEnumerator GetEnumerator()
        {
            CheckClosed();
            CreateConsumer();

            return this;
        }

        public bool MoveNext()
        {
            current = Next();

            if (!session.IsStarted) {
                DestroyConsumer();
                return false;
            }
            
            return current != null;
        }
        
        private IMessage Next() {
            while (true) {
                IMessageConsumer consumer = this.consumer;
                if (consumer == null) {
                    return null;
                }

                IMessage next = null;

                try {
                    next = consumer.ReceiveNoWait();
                } catch (NMSException e) {
                    Tracer.WarnFormat("Error while receive the next message: {}", e.Message);
                }

                if (next == null) {
                    DestroyConsumer();
                }

                return next;
            }
        }

        public void Reset()
        {
            CheckClosed();
            DestroyConsumer();
            CreateConsumer();
        }

        public object Current
        {
            get => current;
        }

        public void Dispose()
        {
            Close();
        }

        public void Close()
        {
            if (closed.CompareAndSet(false, true)) {
                DestroyConsumer();
            }
        }

        public string MessageSelector => selector;
        public IQueue Queue => destination;

        private void CheckClosed()
        {
            if (closed)
            {
                throw new IllegalStateException("The MessageConsumer is closed");
            }
        }

        private void CreateConsumer()
        {
            lock (syncRoot)
            {
                if (consumer == null)
                {
                    NmsMessageConsumer messageConsumer = new NmsQueueBrowserMessageConsumer(session.GetNextConsumerId(), session,
                        destination, selector, false);

                    messageConsumer.Init().ConfigureAwait(false).GetAwaiter().GetResult();

                    // Assign only after fully created and initialized.
                    consumer = messageConsumer;
                }
            }
        }
        
        private void DestroyConsumer()
        {
            lock (syncRoot)
            {
                try
                {
                    consumer?.Close();
                }
                catch (NMSException e)
                {
                    Tracer.DebugFormat("Error closing down internal consumer: ", e);
                }
                finally
                {
                    consumer = null;
                }
            }
        }

        public class NmsQueueBrowserMessageConsumer : NmsMessageConsumer
        {
            public NmsQueueBrowserMessageConsumer(NmsConsumerId consumerId, NmsSession session,
                IDestination destination, string selector, bool noLocal) : base(consumerId, session, destination,
                selector, noLocal)
            {
            }

            protected override bool IsBrowser => true;

        }
    }
}

