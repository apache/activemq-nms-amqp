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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS.AMQP.Meta;

namespace Apache.NMS.AMQP.Provider.Failover
{
    public class FailoverRequest
    {
        private readonly FailoverProvider failoverProvider;
        private readonly TaskCompletionSource<bool> taskCompletionSource;
        private readonly long requestTimeout;
        private readonly DateTime requestStarted;
        private CancellationTokenSource cancellationTokenSource;

        public FailoverRequest(FailoverProvider failoverProvider, long requestTimeout)
        {
            this.failoverProvider = failoverProvider;
            this.requestTimeout = requestTimeout;
            this.requestStarted = DateTime.UtcNow;
            this.taskCompletionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            this.failoverProvider.AddFailoverRequest(this);
        }

        public Func<IProvider, Task> DoTask { get; set; }
        public bool FailureWhenOffline { get; set; } = false;
        public bool SucceedsWhenOffline { get; set; } = false;
        public string Name { get; set; }
        public Task Task => taskCompletionSource.Task;

        public async Task Run()
        {
            // Snapshot the current provider as this action is scoped to that
            // instance and any failure we report should reflect the provider
            // that was in use when the failure happened.
            IProvider activeProvider = failoverProvider.ActiveProvider;

            if (activeProvider == null)
            {
                WhenOffline(new IOException("Connection failed."));
            }
            else
            {
                try
                {
                    await this.DoTask(activeProvider).ConfigureAwait(false);
                    this.taskCompletionSource.TrySetResult(true);
                    this.failoverProvider.RemoveFailoverRequest(this);
                    this.cancellationTokenSource?.Dispose();
                }
                catch (NMSConnectionException exception)
                {
                    Tracer.Debug($"Caught connection exception while executing task: {this} - {exception.Message}");
                    WhenOffline(exception);
                }
                catch (NMSException exception)
                {
                    this.failoverProvider.RemoveFailoverRequest(this);
                    this.taskCompletionSource.TrySetException(exception);
                }
                catch (Exception exception)
                {
                    Tracer.Debug($"Caught exception while executing task: {this} - {exception.Message}");
                    WhenOffline(exception);
                }
            }
        }
        
        public void ScheduleTimeout()
        {
            if (cancellationTokenSource == null && requestTimeout != NmsConnectionInfo.INFINITE)
            {
                TimeSpan timeout = TimeSpan.FromMilliseconds(requestTimeout) - (DateTime.UtcNow - requestStarted);
                if (timeout > TimeSpan.Zero)
                {
                    cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(requestTimeout));
                    cancellationTokenSource.Token.Register(ExpireTask);                    
                }
                else
                {
                    ExpireTask();
                }
            }
        }

        private void ExpireTask()
        {
            if (this.taskCompletionSource.TrySetException(new NMSException($"Timed out waiting on {this}")))
            {
                this.failoverProvider.RemoveFailoverRequest(this);
            }
        }

        private void WhenOffline(Exception exception)
        {
            if (FailureWhenOffline)
            {
                failoverProvider.RemoveFailoverRequest(this);
                taskCompletionSource.TrySetException(exception);
            }
            else if (SucceedsWhenOffline)
            {
                failoverProvider.RemoveFailoverRequest(this);
                taskCompletionSource.SetResult(true);
            }
            else
            {
                ScheduleTimeout();
                Tracer.Debug($"Failover task held until connection recovered: {this}");
            }
        }

        public override string ToString()
        {
            return string.IsNullOrEmpty(Name) ? nameof(FailoverRequest) : Name;
        }
    }
}