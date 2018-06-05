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
using System.Threading;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.NMS.Util;
using Apache.NMS;



namespace NMS.AMQP.Util
{

    internal delegate void Executable();

    internal interface IExecutable
    {
        Executable GetExecutable();
        void OnFailure(Exception e);
    }

    internal interface IWaitable
    {
        bool IsComplete { get; }

        bool IsCancelled { get; }

        bool Wait();
        bool Wait(TimeSpan timeout);
    }

    /// <summary>
    /// General Dispatch Event to execute code from a DispatchExecutor.
    /// </summary>
    class DispatchEvent : IExecutable
    {
        private Executable exe;

        public virtual Executable Callback
        {
            get { return exe; }
            protected  set { exe = value; }
        }

        internal DispatchEvent() : this(null) { }

        internal DispatchEvent(Executable e) { exe = e; }

        public virtual void OnFailure(Exception e)
        {
            Tracer.WarnFormat("Encountered Exception: {0} stack: {1}.", e.Message, e.StackTrace);
        }

        public Executable GetExecutable()
        {
            return exe;
        }
    }

    #region Waitable Dispatcher Event Class
    /// <summary>
    /// A Dispatch event that is completion aware of its current state. This allows threads other then the Dispatch Executor thread to synchronize with the dispatch event.
    /// </summary>
    internal class WaitableDispatchEvent : DispatchEvent, IWaitable
    {
        private ManualResetEvent handle = new ManualResetEvent(false);

        /* state:
            0 = Not Signaled,
            1 = Signaled,
            2 = Cancelled
        */
        #region EventState
        protected class EventState
        {
            internal static EventState INITIAL = new EventState(0, "INITIAL");
            internal static EventState SIGNALED = new EventState(1, "SIGNALED");
            internal static EventState CANCELLED = new EventState(2, "CANCELLED");
            internal static EventState UNKNOWN = new EventState(-1, "UNKNOWN");

            private static Dictionary<int, EventState> States = new Dictionary<int, EventState>()
            {
                { INITIAL.value, INITIAL },
                { SIGNALED.value, SIGNALED },
                { CANCELLED.value, CANCELLED }
            };

            

            private readonly int value;
            private readonly string name;
            private EventState(int ordinal, string name = null)
            {
                value = ordinal;
                this.name = name ?? ordinal.ToString();
            }

            public static implicit operator int(EventState es)
            {
                return es != null ? es.value : UNKNOWN.value;
            }

            public static implicit operator EventState(int value)
            {
                if(!States.TryGetValue(value, out EventState state))
                {
                    state = UNKNOWN;
                }

                return state;
            }

            public override int GetHashCode()
            {
                return this.value;
            }

            public override bool Equals(object obj)
            {
                if(obj != null && obj is EventState)
                {
                    return this.value == (obj as EventState).value;
                }
                return false;
            }

            public override string ToString()
            {
                return this.name;
            }
        }
        #endregion

        private Atomic<int> state;

        private Exception failureCause = null;

        public override Executable Callback
        {
            get => base.Callback;
            protected set
            {
                Executable cb;

                if (value == null)
                {
                    cb = () =>
                    {
                        this.Release();
                    };
                }
                else
                {
                    cb = () =>
                    {
                        value.Invoke();
                        this.Release();
                    };
                }

                base.Callback = cb;
            }
        }

        public bool IsComplete => EventState.SIGNALED.Equals(state.Value);

        public bool IsCancelled => EventState.CANCELLED.Equals(state.Value);

        internal WaitableDispatchEvent() : this(null)
        {
        }

        internal WaitableDispatchEvent(Executable e) 
        {
            handle = new ManualResetEvent(false);
            state = new Atomic<int>(EventState.INITIAL);
            
            this.Callback = e;
        }

        public void Reset()
        {
            state.GetAndSet(EventState.INITIAL);
            if (!handle.Reset())
            {
                throw new NMSException("Failed to reset Waitable Event Signal.");
            }
        }

        public void Cancel()
        {
            if (state.CompareAndSet(EventState.INITIAL, EventState.CANCELLED))
            {
                if (!handle.Set())
                {
                    failureCause = new NMSException("Failed to cancel Waitable Event.");
                }
            }
        }

        private void Release()
        {
            if (state.CompareAndSet(EventState.INITIAL, EventState.SIGNALED))
            {
                
                if (!handle.Set())
                {
                    state.GetAndSet(EventState.CANCELLED);
                    failureCause =  new NMSException("Failed to release Waitable Event.");
                }
            }
        }

        public bool Wait()
        {
            return Wait(TimeSpan.Zero);
        }

        public bool Wait(TimeSpan timeout)
        {
            bool signaled = (timeout.Equals(TimeSpan.Zero)) ? handle.WaitOne() : handle.WaitOne(timeout);
            if (state.Value == EventState.CANCELLED)
            {
                signaled = false;
                if (failureCause != null)
                {
                    throw failureCause;
                }
            }
            return signaled;
        }
    }

    #endregion

    /// <summary>
    /// Single Thread Executor for Dispatch Event. This Encapsulates Threading restrictions for Client code serialization.
    /// </summary>
    class DispatchExecutor : NMSResource, IDisposable
    {
        private static AtomicSequence ExecutorId = new AtomicSequence(1);
        private const string ExecutorName = "DispatchExecutor";
        
        private const int DEFAULT_SIZE = 100000;
        private const int DEFAULT_DEQUEUE_TIMEOUT = 10000;
        private Queue<IExecutable> queue;
        private int maxSize;
        private bool closed=false;
        private Atomic<bool> closeQueued = new Atomic<bool>(false);
        private bool executing=false;
        private Semaphore suspendLock = new Semaphore(0, 10, "Suspend");
        private Thread executingThread;
        private readonly string name;
        private readonly object objLock = new object();

        #region Constructors

        public DispatchExecutor() : this(DEFAULT_SIZE) { }

        public DispatchExecutor(bool drain) : this(DEFAULT_SIZE, drain) { }

        public DispatchExecutor(int size, bool drain = false)
        {
            this.maxSize = size;
            this.ExecuteDrain = drain;
            queue = new Queue<IExecutable>(maxSize);
            executingThread = new Thread(new ThreadStart(this.Dispatch));
            executingThread.IsBackground = true;
            name = ExecutorName + ExecutorId.getAndIncrement() + ":" + executingThread.ManagedThreadId;
            executingThread.Name = name;
        }

        ~DispatchExecutor()
        {
            try
            {
                Dispose(false);
            }
            catch (Exception ex)
            {
                Tracer.DebugFormat("Caught exception in Finalizer for Dispatcher : {0}. Exception {1}", this.name, ex);
            }
        }

        #endregion

        #region Properties

        protected object ThisLock { get { return objLock; } }

        protected bool Closing { get { return closeQueued.Value; } }

        public string Name { get { return name; } }

        internal bool ExecuteDrain { get; private set; }

        internal bool IsOnDispatchThread
        {
            get
            {
                string currentThreadName = Thread.CurrentThread.Name;
                return currentThreadName != null && currentThreadName.Equals(name);
            }
        }

        #endregion

        #region Private Suspend Resume Methods

#if TRACELOCKS
        int scount = 0;
#endif

        protected void Suspend()
        {
            Exception e=null;
            while(!AcquireSuspendLock(out e) && !closed)
            {
                if (e != null)
                {
                    throw e;
                }
            }
        }

        protected bool AcquireSuspendLock()
        {
            Exception e;
            return AcquireSuspendLock(out e);
        }

        protected bool AcquireSuspendLock(out Exception ex)
        {
            bool signaled = false;
            ex = null;
            try
            {
#if TRACELOCKS
                Tracer.InfoFormat("Aquiring Suspend Lock Count {0}", scount);
#endif
                signaled = this.suspendLock.WaitOne();
#if TRACELOCKS
                scount = signaled ? scount - 1 : scount;
#endif
            }catch(Exception e)
            {
                ex = e;
            }
#if TRACELOCKS
            finally
            {
                Tracer.InfoFormat("Suspend Lock Count after aquire {0} signaled {1}", scount, signaled);
            }
#endif
            return signaled;
        }

        protected void Resume()
        {
            Exception ex;
            int count = ReleaseSuspendLock(out ex);
            if (ex != null)
            {
                throw ex;
            }
        }

        protected int ReleaseSuspendLock()
        {
            Exception e;
            return ReleaseSuspendLock(out e);
        }

        protected int ReleaseSuspendLock(out Exception ex)
        {
            ex = null;
            int previous = -1;
            try
            {
#if TRACELOCKS
                Tracer.InfoFormat("Suspend Lock Count before release {0}", scount);
#endif
                previous = this.suspendLock.Release();
#if TRACELOCKS
                scount = previous != -1 ? scount + 1 : scount;
                Tracer.InfoFormat("Suspend Lock Count after release {0} previous Value {1}", scount, previous);
#endif
            }
            catch (SemaphoreFullException sfe)
            {
                // ignore multiple resume calls
                // Log for debugging
                Tracer.DebugFormat("Multiple Resume called on running Dispatcher. Cause: {0}", sfe.Message);
                
            }
            catch(System.IO.IOException ioe)
            {
                Tracer.ErrorFormat("Failed resuming or starting Dispatch thread. Cause: {0}", ioe.Message);
                ex = ioe;
            }
            catch(UnauthorizedAccessException uae)
            {
                Tracer.Error(uae.StackTrace);
                ex =  uae;
            }
            if(ex!=null)
                Console.WriteLine("Release Error {0}", ex);
            return previous;
        }

        #endregion

        #region Protected Dispatch Methods

        protected void CloseOnQueue()
        {
            bool ifDrain = false;
            bool executeDrain = false;
            lock (queue)
            {
                if (!closed)
                {
                    Stop();
                    closed = true;
                    executing = false;
                    ifDrain = true;
                    executeDrain = ExecuteDrain;
                    Monitor.PulseAll(queue);
                    Tracer.InfoFormat("DistpachExecutor: {0} Closed.", name);
                }
            
            }
            if (ifDrain)
            {
                // Drain the rest of the queue before closing
                Drain(executeDrain);
            }
        }

        protected IExecutable[] DrainOffQueue()
        {
            lock (queue)
            {
                ArrayList list = new ArrayList(queue.Count);
                while (queue.Count > 0)
                {
                    list.Add(queue.Dequeue());
                }
                return (IExecutable[])list.ToArray(typeof(IExecutable));
            }
        }

        protected void Drain(bool execute = false)
        {
            IExecutable[] exes = DrainOffQueue();
            if (execute)
            {
                foreach (IExecutable exe in exes)
                {
                    DispatchEvent(exe);
                }
            }
        }

        protected void DispatchEvent(IExecutable dispatchEvent)
        {
            Executable exe = dispatchEvent.GetExecutable();
            if (exe != null)
            {
                try
                {
                    exe.Invoke();
                }
                catch (Exception e)
                {
                    // connect to exception listener here.
                    dispatchEvent.OnFailure(ExceptionSupport.Wrap(e, "Dispatch Executor Error ({0}):", this.name));
                    
                }
            }
        }

        protected void Dispatch()
        {
            while (!closed)
            {
                bool locked = false;
                while (!closed && !(locked = this.AcquireSuspendLock())) { }
                if (locked)
                {
                    int count = this.ReleaseSuspendLock();
#if TraceLocks
                    Tracer.InfoFormat("Dispatch Suspend Lock Count {0}, Current Count {1}", count, count+1);
#endif
                }
                if (closed)
                {
                    break;
                }

                while (IsStarted)
                {
                    
                    IExecutable exe;
                    if (TryDequeue(out exe, DEFAULT_DEQUEUE_TIMEOUT))
                    {
                        
                        DispatchEvent(exe);
                    }
                    else
                    {
                        // queue stopped or timed out
                        Tracer.DebugFormat("Queue {0} did not dispatch due to being Suspended or Closed.", name);
                    }
                }

            }

        }

#endregion

        #region NMSResource Methods

        protected override void StartResource()
        {
            if (!executing)
            {
                executing = true;
                executingThread.Start();
                Resume();
            }
            else
            {
                Resume();
            }
        }

        protected override void StopResource()
        {
            
            lock (queue)
            {
                if (queue.Count == 0)
                {
                    Monitor.PulseAll(queue);
                }
            }
            
            Suspend();
        }

        protected override void ThrowIfClosed()
        {
            if (closed)
            {
                throw new Apache.NMS.IllegalStateException("Illegal Operation on closed " + this.GetType().Name + ".");
            }
        }
        
        #endregion
        
        #region Public Methods

        /// <summary>
        /// Closes the Dispatch Executor. See <see cref="DispatchExecutor.Shutdown(bool)"/>.
        /// </summary>
        public void Close()
        {
            this.Dispose(true);
            //this.Shutdown(true);
        }

        public void Enqueue(IExecutable o)
        {
            if(o == null)
            {
                return;
            }
            lock (queue)
            {
                while (queue.Count >= maxSize)
                {
                    if (closed)
                    {
                        return;
                    }
                    Monitor.Wait(queue);
                }
                queue.Enqueue(o);
                if (queue.Count == 1)
                {
                    Monitor.PulseAll(queue);
                }
                
            }
        }

        public bool TryDequeue(out IExecutable exe, int timeout = -1)
        {
            exe = null;
            lock (queue)
            {
                bool signaled = true;
                while (queue.Count == 0)
                {
                    if (closed || mode.Value.Equals(Resource.Mode.Stopping))
                    {
                        return false;
                    }
                    signaled = (timeout > -1 ) ? Monitor.Wait(queue, timeout) : Monitor.Wait(queue);
                }
                if (!signaled)
                {
                    return false;
                }

                exe = queue.Dequeue();
                if (queue.Count == maxSize - 1)
                {
                    Monitor.PulseAll(queue);
                }
                
            }
            return true;
        }

        #endregion

        #region IDispose Methods

        /// <summary>
        /// Shudowns down the dispatch Thread.
        /// </summary>
        /// <param name="join">
        /// True, indicates whether the shutdown is orderly and therfore can block to join the thread.
        /// False, indicates the shutdown can not block.
        /// Default value is False.
        /// </param>
        internal void Shutdown(bool join = false)
        {
            if (IsOnDispatchThread)
            {
                // close is called in the Dispatcher Thread so we can just close
                if (false == closeQueued.GetAndSet(true))
                {
                    this.CloseOnQueue();
                }
            }
            else if (closeQueued.CompareAndSet(false, true))
            {
                if (!IsStarted && executing)
                {
                    // resume dispatching thread for Close Message Dispatch Event
                    Start();
                }
                // enqueue close
                this.Enqueue(new DispatchEvent(this.CloseOnQueue));

                if (join && executingThread != null)
                {
                    // thread join must not happen under lock (queue) statement
                    if (!executingThread.ThreadState.Equals(ThreadState.Unstarted))
                    {
                        executingThread.Join();
                    }
                    executingThread = null;
                }

            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (closed) return;
            lock (queue)
            {
                if (closed) return;
            }
            if (disposing)
            {
                // remove reference to dispatcher to be garbage collected
                if (executingThread != null && executingThread.ThreadState.Equals(ThreadState.Unstarted))
                {
                    executingThread = null;
                }
                this.Shutdown(true);
                this.suspendLock.Dispose();
                this.queue = null;
            }
            else
            {
                this.Shutdown();
                this.suspendLock.Dispose();
                this.queue = null;
            }
        }

        public void Dispose()
        {
            try
            {
                this.Close();
            }
            catch (Exception ex)
            {
                Tracer.DebugFormat("Caught Exception during Dispose for Dispatcher {0}. Exception {1}", this.name, ex);
            }
        }

        #endregion
        
    }
}
