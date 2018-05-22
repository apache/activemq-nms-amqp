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

namespace NMS.AMQP
{
    using Util;
    namespace Resource
    {
        public enum Mode
        {
            Stopped,
            Starting,
            Started,
            Stopping
        }
    }

    internal abstract class NMSResource : NMSResource<ResourceInfo>
    {

    }

    /// <summary>
    /// NMSResource abstracts the Implementation of IStartable and IStopable for Key NMS class implemetations.
    /// Eg, Connection, Session, MessageConsumer, MessageProducer, etc.
    /// It layouts a foundation for a state machine given by the states in NMS.AMQP.Resource.Mode where 
    /// in general the transitions are Stopped->Starting->Started->Stopping->Stopped->...
    /// </summary>
    internal abstract class NMSResource<T> : IStartable, IStoppable where T : ResourceInfo
    {
        private T info;
        protected T Info
        {
            get { return info; }
            set
            {
                if (value != null)
                {
                    info = value;
                }
            }
        }

        public virtual Id Id
        {
            get
            {
                if(info != null)
                {
                    return info.Id;
                }
                return null;
            }
        }

        protected NMSResource() { }

        protected Atomic<Resource.Mode> mode = new Atomic<Resource.Mode>(Resource.Mode.Stopped);

        public virtual Boolean IsStarted { get { return mode.Value.Equals(Resource.Mode.Started); } }

        protected abstract void StartResource();
        protected abstract void StopResource();
        protected abstract void ThrowIfClosed();

        public void Start()
        {
            ThrowIfClosed();
            if (!IsStarted && mode.CompareAndSet(Resource.Mode.Stopped, Resource.Mode.Starting))
            {
                Resource.Mode finishedMode = Resource.Mode.Stopped;
                try
                {
                    this.StartResource();
                    finishedMode = Resource.Mode.Started;
                }
                catch (Exception e)
                {
                    if(e is NMSException)
                    {
                        throw e;
                    }
                    else
                    {
                        throw ExceptionSupport.Wrap(e, "Failed to Start resource.");
                    }
                }
                finally
                {
                    this.mode.GetAndSet(finishedMode);
                }
            }
        }

        public void Stop()
        {
            ThrowIfClosed();
            if (mode.CompareAndSet(Resource.Mode.Started, Resource.Mode.Stopping))
            {
                Resource.Mode finishedMode = Resource.Mode.Started;
                try
                {
                    this.StopResource();
                    finishedMode = Resource.Mode.Stopped;
                }
                catch (Exception e)
                {
                    if (e is NMSException)
                    {
                        throw e;
                    }
                    else
                    {
                        throw ExceptionSupport.Wrap(e, "Failed to Stop resource.");
                    }
                }
                finally
                {
                    this.mode.GetAndSet(finishedMode);
                }
            }
        }
    }

    internal abstract class ResourceInfo
    {

        private readonly Id resourceId;

        protected ResourceInfo(Id resourceId)
        {
            this.resourceId = resourceId;
        }

        public virtual Id Id { get { return resourceId; } }
    }
}
