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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.NMS;
using Apache.NMS.Util;
using Amqp;
using Amqp.Framing;
using System.Reflection;
using Apache.NMS.AMQP.Util;
using System.Collections.Specialized;

namespace Apache.NMS.AMQP
{

    internal enum LinkState
    {
        UNKNOWN = -1,
        INITIAL = 0,
        ATTACHSENT = 1,
        ATTACHED = 2,
        DETACHSENT = 3,
        DETACHED = 4
    }

    internal enum TerminusDurability
    {
        NONE = 0,
        CONFIGURATION = 1,
        UNSETTLED_STATE = 2,
    }
    
    /// <summary>
    /// Abstract Template for AmqpNetLite Amqp.ILink.
    /// This class Templates the performative Attach and Detached process for the amqp procotol engine class.
    /// The template operations are Attach and Detach.
    /// </summary>
    abstract class MessageLink : NMSResource<LinkInfo>
    {
        private CountDownLatch responseLatch=null;
        private ILink impl;
        private Atomic<LinkState> state = new Atomic<LinkState>(LinkState.INITIAL);
        private readonly Session session;
        private readonly IDestination destination;
        private System.Threading.ManualResetEvent PerformativeOpenEvent = new System.Threading.ManualResetEvent(false);

        protected MessageLink(Session ses, Destination dest)
        {
            session = ses;
            destination = dest;
        }

        protected MessageLink(Session ses, IDestination dest)
        {
            session = ses;
            if(dest is Destination || dest == null)
            {
                destination = dest as Destination;
            }
            else
            {
                if (!dest.IsTemporary)
                {
                    if(dest.IsQueue)
                    {
                        destination = Session.GetQueue((dest as IQueue).QueueName) as Destination;
                    }
                    else
                    {
                        destination = Session.GetQueue((dest as ITopic).TopicName) as Destination;
                    }
                    
                }
                else
                {
                    throw new NotImplementedException("Foreign temporary Destination Implementation Not Supported.");
                }
            }
            
        }

        internal virtual Session Session { get => session; }
        
        internal IDestination Destination { get { return destination; } }

        protected LinkState State { get => this.state.Value; }

        protected ILink Link
        {
            get { return impl; }
            private set {  }
        }
        
        internal bool IsClosing { get { return state.Value.Equals(LinkState.DETACHSENT); } }

        internal bool IsClosed { get { return state.Value.Equals(LinkState.DETACHED); } }

        protected bool IsConfigurable { get { return state.Value.Equals(LinkState.INITIAL); } }

        protected bool IsOpening { get { return state.Value.Equals(LinkState.ATTACHSENT); } }

        protected bool IsRequestPending { get => this.responseLatch != null; }

        internal NMSException FailureCause { get; set; } = null;

        internal void SetFailureCause(Error error, string reason = "")
        {
            this.FailureCause = ExceptionSupport.GetException(error, reason);
        }

        internal void Attach()
        {
            if (state.CompareAndSet(LinkState.INITIAL, LinkState.ATTACHSENT))
            {
                PerformativeOpenEvent.Reset();
                responseLatch = new CountDownLatch(1);
                impl = CreateLink();
                this.Link.AddClosedCallback(this.OnInternalClosed);
                LinkState finishedState = LinkState.UNKNOWN;
                try
                {
                    bool received = true;
                    if (this.Info.requestTimeout <= 0)
                    {
                        responseLatch.await();
                    }
                    else
                    {
                        received = responseLatch.await(RequestTimeout);
                    }
                    if(received && this.impl.Error == null)
                    {
                        finishedState = LinkState.ATTACHED;
                    }
                    else
                    {
                        finishedState = LinkState.INITIAL;
                        if (!received)
                        {
                            Tracer.InfoFormat("Link {0} Attach timeout", Info.Id);
                            this.OnTimeout();
                        }
                        else
                        {
                            Tracer.InfoFormat("Link {0} Attach error: {1}", Info.Id, this.impl.Error);
                            this.OnFailure();
                        }
                    }


                }
                finally
                {
                    responseLatch = null;
                    state.GetAndSet(finishedState);
                    if(!state.Value.Equals(LinkState.ATTACHED) && !this.impl.IsClosed)
                    {
                        DoClose();
                    }
                    PerformativeOpenEvent.Set();
                }
                
            }
        }

        protected virtual void Detach()
        {
            if (state.CompareAndSet(LinkState.ATTACHED, LinkState.DETACHSENT))
            {
                try
                {
                    DoClose();
                }
                catch (NMSException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw ExceptionSupport.Wrap(ex, "Failed to close Link {0}", this.Id);
                }
            }
            else if (state.CompareAndSet(LinkState.INITIAL, LinkState.DETACHED))
            {
                // Link has not been established yet set state to dettached.
            }
            else if (state.Value.Equals(LinkState.ATTACHSENT))
            {
                // The Message Link is trying to estalish a link. It should wait until the Attach response is processed.
                bool signaled = this.PerformativeOpenEvent.WaitOne(this.RequestTimeout);
                if (signaled)
                {
                    if (state.CompareAndSet(LinkState.ATTACHED, LinkState.DETACHSENT))
                    {
                        // The Attach request completed succesfully establishing a link.
                        // Now Close link.
                        try
                        {
                            DoClose();
                        }
                        catch (NMSException)
                        {
                            throw;
                        }
                        catch (Exception ex)
                        {
                            throw ExceptionSupport.Wrap(ex, "Failed to close Link {0}", this.Id);
                        }
                    }
                    else if (state.CompareAndSet(LinkState.INITIAL, LinkState.DETACHED))
                    {
                        // Failed to establish a link set state to Detached.
                    }
                }
                else
                {
                    // Failed to receive establishment event signal.
                    state.GetAndSet(LinkState.DETACHED);
                }
                    

            }
        }

        /// <summary>
        /// Defines the asynchronous Amqp.ILink error and close notification handler for the template.
        /// This Method matches the delegate <see cref="Amqp.ClosedCallback"/>.
        /// Concrete implementations are required to implement this method.
        /// </summary>
        /// <param name="sender">
        /// The <see cref="Amqp.IAmqpObject"/> that has closed. Also, <seealso cref="Amqp.ClosedCallback"/>.
        /// This will always be an ILink for the template.
        /// </param>
        /// <param name="error">
        /// The <see cref="Amqp.Framing.Error"/> that caused the link to close.
        /// This can be null should the link be closed intentially.
        /// </param>
        protected virtual void OnInternalClosed(Amqp.IAmqpObject sender, Error error)
        {
            bool failureThrow = true;
            Session parent = null;
            string name = (sender as ILink)?.Name;
            NMSException failure = ExceptionSupport.GetException(error, 
                "Received Amqp link detach with Error for link {0}", name); 

            try
            {
                parent = this.Session;
                this.FailureCause = failure;
                if (this.state.CompareAndSet(LinkState.DETACHSENT, LinkState.DETACHED))
                {
                    // expected close
                    parent.Remove(this);
                }
                else if (this.state.CompareAndSet(LinkState.ATTACHED, LinkState.DETACHED))
                {
                    // unexpected close or parent close
                    if (this.IsStarted)
                    {
                        // indicate unexpected close
                        this.Shutdown();
                    }
                    else
                    {
                        failureThrow = false;
                    }

                    parent.Remove(this);
                }
            }
            catch (Exception ex)
            {
                // failure during NMS object instance cleanup.
                Tracer.DebugFormat("Caught exception during Amqp Link {0} close. Exception {1}", name, ex);
            }
            
            // Determine if there is a provider controlled call for this closed callback.
            if (!failureThrow && failure != null)
            {
                // no provider controlled call.
                // Log error if any.
                
                // Log exception.
                Tracer.InfoFormat("Amqp Performative detach response error. Message {0}", failure.Message);
                
            }
        }

        /// <summary>
        /// Defines the link create operation for the abstract template.
        /// Concrete implmentations are required to implement this method.
        /// </summary>
        /// <returns>
        /// An ILink that was configured by concrete implementation.
        /// </returns>
        protected abstract ILink CreateLink();

        /// <summary>
        /// See <see cref="MessageLink.DoClose(TimeSpan, Error)"/>.
        /// </summary>
        /// <param name="cause"></param>
        protected void DoClose(Error cause = null)
        {
            this.DoClose(TimeSpan.FromMilliseconds(Info.closeTimeout), cause);
        }

        /// <summary>
        /// Defines the link close operation for the abstract template.
        /// Concrete implementation can implement this method to override the default Link close operation.
        /// </summary>
        /// <param name="timeout">
        /// Timeout on network operation close for link. 
        /// If greater then 0 then operation will be blocking.
        /// Otherwise the network operation is non-blocking.
        /// </param>
        /// <param name="cause">
        /// The amqp Error that caused the link to close.
        /// </param>
        /// <exception cref="Amqp.AmqpException">Throws for Detach response errors.</exception>
        /// <exception cref="System.TimeoutException">Throws when timeout expires for blocking detach request.</exception>
        protected virtual void DoClose(TimeSpan timeout, Error cause = null)
        {
            if (this.impl != null && !this.impl.IsClosed)
            {
                Tracer.DebugFormat("Detaching amqp link {0} for {1} with timeout {2}", this.Link.Name, this.Id, timeout);
                this.impl.Close(timeout, cause);
            }
        }

        protected virtual void OnResponse()
        {
            if (responseLatch != null)
            {
                responseLatch.countDown();
            }
        }

        protected virtual void OnTimeout()
        {
            throw ExceptionSupport.GetTimeoutException(this.impl, "Performative Attach Timeout while waiting for response.");
        }

        protected virtual void OnFailure()
        {
            throw ExceptionSupport.GetException(this.impl, "Performative Attach Error.");
        }

        protected virtual void Configure()
        {
            StringDictionary connProps = Session.Connection.Properties;
            StringDictionary sessProps = Session.Properties;
            PropertyUtil.SetProperties(Info, connProps);
            PropertyUtil.SetProperties(Info, sessProps);

        }


        #region NMSResource Methhods

        protected override void StartResource()
        {
            this.Attach();
        }
        
        protected override void ThrowIfClosed()
        {
            if (state.Value.Equals(LinkState.DETACHED) || this.FailureCause != null)
            {
                throw new Apache.NMS.IllegalStateException("Illegal operation on closed I" + this.GetType().Name + ".", this.FailureCause);
            }
        }

        #endregion

        #region Public Inheritable Properties

        public TimeSpan RequestTimeout
        {
            get { return TimeSpan.FromMilliseconds(Info.requestTimeout); }
            set { Info.requestTimeout = Convert.ToInt64(value.TotalMilliseconds); }
        }

        #endregion

        #region Public Inheritable Methods

        // All derived classes from MessageLink must be IDisposable.
        // Use the IDispoable pattern for Close() and have Dispose() just call Close() and Close() do
        // the things usually done in Dispose().
        public void Close()
        {
            this.Dispose(true);
            if (IsClosed)
            {
                GC.SuppressFinalize(this);
            }
        }
        /// <summary>
        /// Implements a template for the IDisposable Parttern. 
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (this.IsClosed) return;
            if (disposing)
            {
                // orderly shutdown
                this.Stop();
                this.Shutdown();
                try
                {
                    this.Detach();
                }
                catch (Exception ex)
                {
                    // Log network errors
                    Tracer.DebugFormat("Performative detached raised exception for {0} {1}. Message {2}",
                        this.GetType().Name, this.Id, ex.Message);
                }
                finally
                {
                    // in case detach failed or times out.
                    if (!this.IsClosed)
                    {
                        this.Session.Remove(this);
                        this.state.Value = LinkState.DETACHED;
                    }
                    this.impl = null;
                }
            }
        }

        #endregion

        #region Shutdown Template methods

        /// <summary>
        /// Shutdown orderly shuts down the Message link facilities.
        /// </summary>
        internal virtual void Shutdown()
        {
        }

        
        #endregion
    }

    #region LinkInfo Class

    internal abstract class LinkInfo : ResourceInfo
    {
        protected static readonly long DEFAULT_REQUEST_TIMEOUT;
        static LinkInfo()
        {
            DEFAULT_REQUEST_TIMEOUT = Convert.ToInt64(NMSConstants.defaultRequestTimeout.TotalMilliseconds);
        }

        protected LinkInfo(Id linkId) : base(linkId)
        {

        }

        public long requestTimeout { get; set; } = DEFAULT_REQUEST_TIMEOUT;
        public long closeTimeout { get; set; } = DEFAULT_REQUEST_TIMEOUT;
        public long sendTimeout { get; set; }

        public override string ToString()
        {
            string result = "";
            result += "LinkInfo = [\n";
            foreach (MemberInfo info in this.GetType().GetMembers())
            {
                if (info is PropertyInfo)
                {
                    PropertyInfo prop = info as PropertyInfo;
                    if (prop.GetGetMethod(true).IsPublic)
                    {
                        result += string.Format("{0} = {1},\n", prop.Name, prop.GetValue(this, null));
                    }
                }
            }
            result = result.Substring(0, result.Length - 2) + "\n]";
            return result;
        }

    }

    #endregion
}
