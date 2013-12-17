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
using System.Threading;
using Apache.NMS.Util;
using Org.Apache.Qpid.Messaging;

namespace Apache.NMS.Amqp
{
    /// <summary>
    /// Represents a NMS Qpid/Amqp connection.
    /// </summary>
    ///
    public class Connection : IConnection
    {
        private static readonly TimeSpan InfiniteTimeSpan = TimeSpan.FromMilliseconds(Timeout.Infinite);

        private AcknowledgementMode acknowledgementMode = AcknowledgementMode.AutoAcknowledge;
        private IMessageConverter messageConverter = new DefaultMessageConverter();

        private IRedeliveryPolicy redeliveryPolicy;
        private ConnectionMetaData metaData = null;

        private readonly object connectedLock = new object();
        private readonly Atomic<bool> connected = new Atomic<bool>(false);
        private readonly Atomic<bool> closed = new Atomic<bool>(false);
        private readonly Atomic<bool> closing = new Atomic<bool>(false);

        private readonly Atomic<bool> started = new Atomic<bool>(false);
        private bool disposed = false;
        private string clientId;
        private Uri brokerUri;

        private int sessionCounter = 0; 
        private readonly IList sessions = ArrayList.Synchronized(new ArrayList());

        Org.Apache.Qpid.Messaging.Connection qpidConnection = null; // Don't create until Start()

        /// <summary>
        /// Creates new connection
        /// </summary>
        /// <param name="connectionUri"></param>
        public Connection(Uri connectionUri)
        {
            this.brokerUri = connectionUri;
        }

        /// <summary>
        /// Destroys connection
        /// </summary>
        ~Connection()
        {
            Dispose(false);
        }

        /// <summary>
        /// Starts message delivery for this connection.
        /// </summary>
        public void Start()
        {
            CheckConnected();
            if (started.CompareAndSet(false, true))
            {
                lock (sessions.SyncRoot)
                {
                    foreach (Session session in sessions)
                    {
                        //session.Start();
                    }
                }
            }
        }

        /// <summary>
        /// This property determines if the asynchronous message delivery of incoming
        /// messages has been started for this connection.
        /// </summary>
        public bool IsStarted
        {
            get { return started.Value; }
        }

        /// <summary>
        /// Temporarily stop asynchronous delivery of inbound messages for this connection.
        /// The sending of outbound messages is unaffected.
        /// </summary>
        public void Stop()
        {
            if (started.CompareAndSet(true, false))
            {
                lock (sessions.SyncRoot)
                {
                    foreach (Session session in sessions)
                    {
                        //session.Stop();
                    }
                }
            }
        }

        /// <summary>
        /// Creates a new session to work on this connection
        /// </summary>
        public ISession CreateSession()
        {
            return CreateSession(acknowledgementMode);
        }

        /// <summary>
        /// Creates a new session to work on this connection
        /// </summary>
        public ISession CreateSession(AcknowledgementMode mode)
        {
            CheckConnected();
            return new Session(this, GetNextSessionId(), mode);
        }

        internal void AddSession(Session session)
        {
            if (!this.closing.Value)
            {
                sessions.Add(session);
            }
        }

        internal void RemoveSession(Session session)
        {
            if (!this.closing.Value)
            {
                sessions.Remove(session);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            if (disposed)
            {
                return;
            }

            if (disposing)
            {
                // Dispose managed code here.
            }

            try
            {
                Close();
            }
            catch
            {
                // Ignore network errors.
            }

            disposed = true;
        }

        /// <summary>
        /// Get/or set the broker Uri.
        /// </summary>
        public Uri BrokerUri
        {
            get { return brokerUri; }
            set { brokerUri = value; }
        }

        /// <summary>
        /// The default timeout for network requests.
        /// </summary>
        public TimeSpan RequestTimeout
        {
            get { return NMSConstants.defaultRequestTimeout; }
            set { }
        }

        public AcknowledgementMode AcknowledgementMode
        {
            get { return acknowledgementMode; }
            set { acknowledgementMode = value; }
        }

        public IMessageConverter MessageConverter
        {
            get { return messageConverter; }
            set { messageConverter = value; }
        }

        public string ClientId
        {
            get { return clientId; }
            set
            {
                if(connected.Value)
                {
                    throw new NMSException("You cannot change the ClientId once the Connection is connected");
                }
                clientId = value;
            }
        }

        /// <summary>
        /// Get/or set the redelivery policy for this connection.
        /// </summary>
        public IRedeliveryPolicy RedeliveryPolicy
        {
            get { return this.redeliveryPolicy; }
            set { this.redeliveryPolicy = value; }
        }

        private ConsumerTransformerDelegate consumerTransformer;
        public ConsumerTransformerDelegate ConsumerTransformer
        {
            get { return this.consumerTransformer; }
            set { this.consumerTransformer = value; }
        }

        private ProducerTransformerDelegate producerTransformer;
        public ProducerTransformerDelegate ProducerTransformer
        {
            get { return this.producerTransformer; }
            set { this.producerTransformer = value; }
        }

        /// <summary>
        /// Gets the Meta Data for the NMS Connection instance.
        /// </summary>
        public IConnectionMetaData MetaData
        {
            get { return this.metaData ?? (this.metaData = new ConnectionMetaData()); }
        }

        /// <summary>
        /// A delegate that can receive transport level exceptions.
        /// </summary>
        public event ExceptionListener ExceptionListener;

        /// <summary>
        /// An asynchronous listener that is notified when a Fault tolerant connection
        /// has been interrupted.
        /// </summary>
        public event ConnectionInterruptedListener ConnectionInterruptedListener;

        /// <summary>
        /// An asynchronous listener that is notified when a Fault tolerant connection
        /// has been resumed.
        /// </summary>
        public event ConnectionResumedListener ConnectionResumedListener;

        /// <summary>
        /// Check and ensure that the connection object is connected.  
        /// New connections are established for the first time.
        /// Subsequent calls verify that connection is connected and is not closed or closing.
        /// This function returns only if connection is successfully opened else
        /// a ConnectionClosedException is thrown.
        /// </summary>
        internal void CheckConnected()
        {
            if (closed.Value || closing.Value)
            {
                throw new ConnectionClosedException();
            }
            if (connected.Value)
            {
                return;
            }
            DateTime timeoutTime = DateTime.Now + this.RequestTimeout;
            int waitCount = 1;

            while (!connected.Value && !closed.Value && !closing.Value)
            {
                if (Monitor.TryEnter(connectedLock))
                {
                    try // strictly for Monitor unlock
                    {
                        // Create and open the Qpid connection
                        try
                        {
                            // TODO: embellish the brokerUri with other connection options
                            // Allocate a new Qpid connection
                            qpidConnection = new Org.Apache.Qpid.Messaging.Connection(brokerUri.ToString());
                            
                            // Open the connection
                            qpidConnection.Open();

                            connected.Value = true;
                        }
                        catch (Org.Apache.Qpid.Messaging.QpidException e)
                        {
                            throw new ConnectionClosedException( e.Message );
                        }
                    }
                    finally
                    {
                        Monitor.Exit(connectedLock);
                    }
                }

                if (connected.Value || closed.Value || closing.Value
                    || (DateTime.Now > timeoutTime && this.RequestTimeout != InfiniteTimeSpan))
                {
                    break;
                }

                // Back off from being overly aggressive.  Having too many threads
                // aggressively trying to connect to a down broker pegs the CPU.
                Thread.Sleep(5 * (waitCount++));
            }

            if (!connected.Value)
            {
                throw new ConnectionClosedException();
            }
        }

        public void Close()
        {
            Dispose();
        }

        public void PurgeTempDestinations()
        {
        }

        public void HandleException(Exception e)
        {
            if(ExceptionListener != null && !this.closed.Value)
            {
                ExceptionListener(e);
            }
            else
            {
                Tracer.Error(e);
            }
        }

        public int GetNextSessionId()
        {
            return Interlocked.Increment(ref sessionCounter);
        }
    }
}
