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
using System.Collections.Specialized;
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
        // Connections options indexes and constants
        private const string PROTOCOL_OPTION = "protocol";
        private const string PROTOCOL_1_0 = "amqp1.0";
        private const string PROTOCOL_0_10 = "amqp0-10";
        private const char SEP_ARGS = ',';
        private const char SEP_NAME_VALUE = ':';
        public const string USERNAME_OPTION = "username";
        public const string PASSWORD_OPTION = "password";

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

        private Uri brokerUri;
        private string clientId;
        private StringDictionary connectionProperties;

        private int sessionCounter = 0;
        private readonly IList sessions = ArrayList.Synchronized(new ArrayList());

        private Org.Apache.Qpid.Messaging.Connection qpidConnection = null; // Don't create until Start()

        #region Constructor Methods

        /// <summary>
        /// Creates new connection
        /// </summary>
        public Connection()
        {
        }

        /// <summary>
        /// Destroys connection
        /// </summary>
        ~Connection()
        {
            Dispose(false);
        }

        #endregion

        #region IStartable Members
        /// <summary>
        /// Starts message delivery for this connection.
        /// </summary>
        public void Start()
        {
            // Create and open qpidConnection
            CheckConnected();

            if (started.CompareAndSet(false, true))
            {
                lock (sessions.SyncRoot)
                {
                    foreach (Session session in sessions)
                    {
                        // Create and start qpidSessions
                        session.Start();
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
        #endregion

        #region IStoppable Members
        /// <summary>
        /// Temporarily stop asynchronous delivery of inbound messages for this connection.
        /// The sending of outbound messages is unaffected.
        /// </summary>
        public void Stop()
        {
            // Close qpidConnection
            CheckDisconnected();

            // Administratively close NMS objects
            if (started.CompareAndSet(true, false))
            {
                foreach (Session session in sessions)
                {
                    // Create and start qpidSessions
                    session.Stop();
                }
            }
        }
        #endregion

        #region IDisposable Methods
        public void Dispose()
        {
            Dispose(true);
        }
        #endregion

        #region AMQP IConnection Class Methods
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
                ThrowIfConnected("ClientId");
                clientId = value;
            }
        }

        /// <summary>
        /// Get/or set the redelivery policy for this connection.
        /// </summary>
        public IRedeliveryPolicy RedeliveryPolicy
        {
            get { return this.redeliveryPolicy; }
            set
            {
                ThrowIfConnected("RedeliveryPolicy");
                this.redeliveryPolicy = value;
            }
        }

        private ConsumerTransformerDelegate consumerTransformer;
        public ConsumerTransformerDelegate ConsumerTransformer
        {
            get { return this.consumerTransformer; }
            set
            {
                ThrowIfConnected("ConsumerTransformer");
                this.consumerTransformer = value;
            }
        }

        private ProducerTransformerDelegate producerTransformer;
        public ProducerTransformerDelegate ProducerTransformer
        {
            get { return this.producerTransformer; }
            set
            {
                ThrowIfConnected("ProducerTransformer");
                this.producerTransformer = value;
            }
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
                            // Allocate a Qpid connection
                            if (qpidConnection == null)
                            {
                                Tracer.DebugFormat("Amqp: create qpid connection to: {0}", this.BrokerUri.ToString());
                                qpidConnection =
                                    new Org.Apache.Qpid.Messaging.Connection(
                                        brokerUri.ToString(),
                                        ConstructConnectionOptionsString(connectionProperties));
                            }

                            // Open the connection
                            if (!qpidConnection.IsOpen)
                            {
                                qpidConnection.Open();
                            }

                            connected.Value = true;
                        }
                        catch (Org.Apache.Qpid.Messaging.QpidException e)
                        {
                            Tracer.DebugFormat("Amqp: create qpid connection to: {0} failed with {1}", 
                                this.BrokerUri.ToString(), e.Message);
                            throw new ConnectionClosedException(e.Message);
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


        /// <summary>
        /// Check and ensure that the connection object is disconnected
        /// Open connections are closed and this closes related sessions, senders, and receivers.
        /// Closed connections may be restarted with subsequent calls to Start().
        /// </summary>
        internal void CheckDisconnected()
        {
            if (closed.Value || closing.Value)
            {
                throw new ConnectionClosedException();
            }
            if (!connected.Value)
            {
                return;
            }
            while (connected.Value && !closed.Value && !closing.Value)
            {
                if (Monitor.TryEnter(connectedLock))
                {
                    try
                    {
                        // Close the connection
                        if (qpidConnection.IsOpen)
                        {
                            qpidConnection.Close();
                        }

                        connected.Value = false;
                        break;
                    }
                    catch (Org.Apache.Qpid.Messaging.QpidException e)
                    {
                        throw new NMSException("AMQP Connection close failed : " + e.Message);
                    }
                    finally
                    {
                        Monitor.Exit(connectedLock);
                    }
                }
            }

            if (connected.Value)
            {
                throw new NMSException("Failed to close AMQP Connection");
            }
        }

        public void Close()
        {
            if (!this.closed.Value)
            {
                this.Stop();
            }

            lock (connectedLock)
            {
                if (this.closed.Value)
                {
                    return;
                }

                try
                {
                    Tracer.InfoFormat("Connection[]: Closing Connection Now.");
                    this.closing.Value = true;

                    lock (sessions.SyncRoot)
                    {
                        foreach (Session session in sessions)
                        {
                            session.Shutdown();
                        }
                    }
                    sessions.Clear();

                }
                catch (Exception ex)
                {
                    Tracer.ErrorFormat("Connection[]: Error during connection close: {0}", ex);
                }
                finally
                {
                    this.closed.Value = true;
                    this.connected.Value = false;
                    this.closing.Value = false;
                }
            }
        }

        public void PurgeTempDestinations()
        {
        }

        #endregion

        #region ConnectionProperties Methods

        /// <summary>
        /// Connection connectionProperties acceessor
        /// </summary>
        /// <remarks>This factory does not check for legal property names. Users
        /// my specify anything they want. Propery name processing happens when
        /// connections are created and started.</remarks>
        public StringDictionary ConnectionProperties
        {
            get { return connectionProperties; }
            set
            {
                ThrowIfConnected("ConnectionProperties");
                connectionProperties = value;
            }
        }

        /// <summary>
        /// Test existence of named property
        /// </summary>
        /// <param name="name">The name of the connection property to test.</param>
        /// <returns>Boolean indicating if property exists in setting dictionary.</returns>
        public bool ConnectionPropertyExists(string name)
        {
            return connectionProperties.ContainsKey(name);
        }

        /// <summary>
        /// Get value of named property
        /// </summary>
        /// <param name="name">The name of the connection property to get.</param>
        /// <returns>string value of property.</returns>
        /// <remarks>Throws if requested property does not exist.</remarks>
        public string GetConnectionProperty(string name)
        {
            if (connectionProperties.ContainsKey(name))
            {
                return connectionProperties[name];
            }
            else
            {
                throw new NMSException("Amqp connection property '" + name + "' does not exist");
            }
        }

        /// <summary>
        /// Set value of named property
        /// </summary>
        /// <param name="name">The name of the connection property to set.</param>
        /// <param name="value">The value of the connection property.</param>
        /// <returns>void</returns>
        /// <remarks>Existing property values are overwritten. New property values
        /// are added.</remarks>
        public void SetConnectionProperty(string name, string value)
        {
            ThrowIfConnected("SetConnectionProperty:" + name);
            if (connectionProperties.ContainsKey(name))
            {
                connectionProperties[name] = value;
            }
            else
            {
                connectionProperties.Add(name, value);
            }
        }
        #endregion

        #region AMQP Connection Utilities

        private void ThrowIfConnected(string propName)
        {
            if (connected.Value)
            {
                throw new NMSException("Can not change connection property while Connection is connected: " + propName);
            }
        }

        public void HandleException(Exception e)
        {
            if (ExceptionListener != null && !this.closed.Value)
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

        public Org.Apache.Qpid.Messaging.Session CreateQpidSession()
        {
            // TODO: Session name; transactional session
            if (!connected.Value)
            {
                throw new ConnectionClosedException();
            }
            return qpidConnection.CreateSession();
        }


        /// <summary>
        /// Convert specified connection properties string map into the
        /// connection properties string to send to Qpid Messaging. 
        /// </summary>
        /// <returns>qpid connection properties string</returns>
        /// <remarks>Mostly this is pass-through. Default to amqp1.0
        /// in the absence of any protocol option.</remarks>
        internal string ConstructConnectionOptionsString(StringDictionary cp)
        {
            string result = "";
            // Construct qpid connection string
            bool first = true;
            result = "{";
            foreach (DictionaryEntry de in cp)
            {
                if (!first)
                {
                    result += SEP_ARGS;
                }
                result += de.Key + SEP_NAME_VALUE.ToString() + de.Value;
                first = false;
            }

            // protocol version munging
            if (!cp.ContainsKey(PROTOCOL_OPTION))
            {
                // no protocol option - select 1.0
                if (!first)
                {
                    result += SEP_ARGS;
                }
                result += PROTOCOL_OPTION + SEP_NAME_VALUE.ToString() + PROTOCOL_1_0;
            }

            result += "}";
            return result;
        }

        #endregion
    }
}
