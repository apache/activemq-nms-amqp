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
using System.IO;
using System.Threading.Tasks;
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Meta;
using Apache.NMS.AMQP.Util;
using Apache.NMS.Util;

namespace Apache.NMS.AMQP.Provider.Failover
{
    public class FailoverProvider : IProvider, IProviderListener
    {
        private readonly object SyncRoot = new object();

        private static int UNDEFINED = -1;

        public static int DEFAULT_INITIAL_RECONNECT_DELAY = 0;
        public static long DEFAULT_RECONNECT_DELAY = 10;
        public static double DEFAULT_RECONNECT_BACKOFF_MULTIPLIER = 2.0d;
        public static long DEFAULT_MAX_RECONNECT_DELAY = (long) Math.Round(TimeSpan.FromSeconds(30).TotalMilliseconds);
        public static int DEFAULT_STARTUP_MAX_RECONNECT_ATTEMPTS = UNDEFINED;
        public static int DEFAULT_MAX_RECONNECT_ATTEMPTS = UNDEFINED;
        public static bool DEFAULT_USE_RECONNECT_BACKOFF = true;
        public static int DEFAULT_WARN_AFTER_RECONNECT_ATTEMPTS = 10;
        public static bool DEFAULT_RANDOMIZE_ENABLED = false;

        private readonly ReconnectControls reconnectControl;
        private readonly FailoverUriPool uris;

        private readonly AtomicBool closed = new AtomicBool();
        private readonly Atomic<bool> failed = new Atomic<bool>();

        private long requestTimeout;
        private long sendTimeout;

        private Uri connectedUri;
        private NmsConnectionInfo connectionInfo;
        private IProvider provider;
        private IProviderListener listener;

        private List<FailoverRequest> requests = new List<FailoverRequest>();

        internal IProvider ActiveProvider => provider;

        public FailoverProvider(IEnumerable<Uri> uris)
        {
            this.uris = new FailoverUriPool(uris);
            reconnectControl = new ReconnectControls(this);
        }

        public long InitialReconnectDelay { get; set; } = DEFAULT_INITIAL_RECONNECT_DELAY;
        public long ReconnectDelay { get; set; } = DEFAULT_RECONNECT_DELAY;
        public bool UseReconnectBackOff { get; set; } = DEFAULT_USE_RECONNECT_BACKOFF;
        public double ReconnectBackOffMultiplier { get; set; } = DEFAULT_RECONNECT_BACKOFF_MULTIPLIER;
        public long MaxReconnectDelay { get; set; } = DEFAULT_MAX_RECONNECT_DELAY;
        public int StartupMaxReconnectAttempts { get; set; } = DEFAULT_STARTUP_MAX_RECONNECT_ATTEMPTS;
        public int MaxReconnectAttempts { get; set; } = DEFAULT_MAX_RECONNECT_ATTEMPTS;
        public int WarnAfterReconnectAttempts { get; set; } = DEFAULT_WARN_AFTER_RECONNECT_ATTEMPTS;
        public Uri RemoteUri => connectedUri;

        public void Start()
        {
            CheckClosed();

            if (listener == null)
            {
                throw new IllegalStateException("No ProviderListener registered.");
            }
        }

        public Task Connect(NmsConnectionInfo info)
        {
            CheckClosed();

            requestTimeout = info.RequestTimeout;
            sendTimeout = info.SendTimeout;

            connectionInfo = info;
            Tracer.Debug("Initiating initial connection attempt task");
            return TriggerReconnectionAttempt();
        }

        private Task TriggerReconnectionAttempt()
        {
            if (closed)
                return Task.CompletedTask;

            return reconnectControl.ScheduleReconnect(Reconnect);

            async Task Reconnect()
            {
                IProvider provider = null;
                Exception failure = null;
                long reconnectAttempts = reconnectControl.RecordNextAttempt();

                try
                {
                    if (uris.Any())
                    {
                        for (int i = 0; i < uris.Size(); i++)
                        {
                            var target = uris.GetNext();
                            if (target == null)
                            {
                                Tracer.Debug("Failover URI collection unexpectedly modified during connection attempt.");
                                continue;
                            }

                            try
                            {
                                Tracer.Debug($"Connection attempt:[{reconnectAttempts}] to: {target.Scheme}://{target.Host}:{target.Port} in-progress");
                                provider = ProviderFactory.Create(target);
                                await provider.Connect(connectionInfo).ConfigureAwait(false);
                                await InitializeNewConnection(provider).ConfigureAwait(false);
                                return;
                            }
                            catch (Exception e)
                            {
                                Tracer.Info($"Connection attempt:[{reconnectAttempts}] to: {target.Scheme}://{target.Host}:{target.Port} failed");
                                failure = e;
                                try
                                {
                                    provider?.Close();
                                }
                                catch
                                {
                                }
                                finally
                                {
                                    provider = null;
                                }
                            }
                        }
                    }
                    else
                    {
                        Tracer.Debug("No remote URI available to connect to in failover list");
                        // TODO Handle this one.
                        failure = new IOException("No remote URI available for reconnection during connection attempt: " + reconnectAttempts);
                    }
                }
                catch (Exception unknownFailure)
                {
                    Tracer.Warn($"Connection attempt:[{reconnectAttempts}] failed abnormally.");
                    failure = failure ?? unknownFailure;
                }
                finally
                {
                    if (provider == null)
                    {
                        Tracer.Debug($"Connection attempt:[{reconnectControl.ReconnectAttempts}] failed error: {failure?.Message}");
                        if (!reconnectControl.IsReconnectAllowed(failure))
                        {
                            ReportReconnectFailure(failure);
                        }
                        else
                        {
                            await reconnectControl.ScheduleReconnect(Reconnect).ConfigureAwait(false);
                        }
                    }
                }
            }
        }

        private void ReportReconnectFailure(Exception lastFailure)
        {
            Tracer.Error($"Failed to connect after: {reconnectControl.ReconnectAttempts} attempt(s)");
            if (failed.CompareAndSet(false, true))
            {
                if (lastFailure == null)
                {
                    lastFailure = new IOException($"Failed to connect after: {reconnectControl.ReconnectAttempts} attempt(s)");
                }

                var exception = NMSExceptionSupport.Create(lastFailure);
                listener.OnConnectionFailure(exception);
                throw exception;
            }
        }

        private async Task InitializeNewConnection(IProvider provider)
        {
            if (closed)
            {
                try
                {
                    provider.Close();
                }
                catch (Exception e)
                {
                    Tracer.Debug($"Ignoring failure to close failed provider: {provider} {e.Message}");
                }
                return;
            }

            this.provider = provider;
            this.provider.SetProviderListener(this);
            this.connectedUri = provider.RemoteUri;

            if (reconnectControl.IsRecoveryRequired())
            {
                Tracer.Debug($"Signalling connection recovery: {provider}");

                // Allow listener to recover its resources
                await listener.OnConnectionRecovery(provider).ConfigureAwait(false);

                // Restart consumers, send pull commands, etc.
                await listener.OnConnectionRecovered(provider).ConfigureAwait(false);

                // Let the client know that connection has restored.
                listener.OnConnectionRestored(connectedUri);

                // If we try to run pending requests right after the connection is reestablished 
                // it will result in timeout on the first send request
                await Task.Delay(50).ConfigureAwait(false);

                foreach (FailoverRequest request in GetPendingRequests())
                {
                    await request.Run().ConfigureAwait(false);
                }

                reconnectControl.ConnectionEstablished();
            }
            else
            {
                listener.OnConnectionEstablished(connectedUri);
                reconnectControl.ConnectionEstablished();
            }
        }

        public override string ToString()
        {
            return "FailoverProvider: " + (connectedUri == null ? "unconnected" : connectedUri.ToString());
        }

        public void Close()
        {
            if (closed.CompareAndSet(false, true))
            {
                try
                {
                    provider?.Close();
                }
                catch (Exception e)
                {
                    Tracer.Warn("Error caught while closing Provider: " + e.Message);
                }
            }
        }

        public void SetProviderListener(IProviderListener providerListener)
        {
            CheckClosed();

            listener = providerListener;
        }

        public Task CreateResource(INmsResource resourceInfo)
        {
            CheckClosed();

            FailoverRequest request = new FailoverRequest(this, requestTimeout)
            {
                DoTask = activeProvider => provider.CreateResource(resourceInfo),
                Name = nameof(CreateResource)
            };

            request.Run();

            return request.Task;
        }

        public Task DestroyResource(INmsResource resourceInfo)
        {
            CheckClosed();

            FailoverRequest request = new FailoverRequest(this, requestTimeout)
            {
                DoTask = activeProvider => activeProvider.DestroyResource(resourceInfo),
                Name = nameof(DestroyResource),

                // Allow this to succeed, resource won't get recreated on reconnect.
                SucceedsWhenOffline = true
            };

            request.Run();

            return request.Task;
        }

        public Task StartResource(INmsResource resourceInfo)
        {
            CheckClosed();

            FailoverRequest request = new FailoverRequest(this, requestTimeout)
            {
                DoTask = activeProvider => activeProvider.StartResource(resourceInfo),
                Name = nameof(StartResource)
            };

            request.Run();

            return request.Task;
        }

        public Task StopResource(INmsResource resourceInfo)
        {
            CheckClosed();

            FailoverRequest request = new FailoverRequest(this, requestTimeout)
            {
                DoTask = activeProvider => activeProvider.StopResource(resourceInfo),
                Name = nameof(StopResource)
            };

            request.Run();

            return request.Task;
        }

        public Task Recover(NmsSessionId sessionId)
        {
            CheckClosed();

            FailoverRequest request = new FailoverRequest(this, requestTimeout)
            {
                DoTask = activeProvider => activeProvider.Recover(sessionId),
                SucceedsWhenOffline = true,
                Name = nameof(Recover)
            };

            request.Run();

            return request.Task;
        }

        public Task Acknowledge(NmsSessionId sessionId, AckType ackType)
        {
            CheckClosed();

            FailoverRequest request = new FailoverRequest(this, requestTimeout)
            {
                DoTask = activeProvider => activeProvider.Acknowledge(sessionId, ackType),
                FailureWhenOffline = true,
                Name = nameof(Acknowledge)
            };

            request.Run();

            return request.Task;
        }

        public Task Acknowledge(InboundMessageDispatch envelope, AckType ackType)
        {
            CheckClosed();

            FailoverRequest request = new FailoverRequest(this, requestTimeout)
            {
                DoTask = activeProvider => activeProvider.Acknowledge(envelope, ackType),
                FailureWhenOffline = true,
                Name = nameof(Acknowledge)
            };

            request.Run();

            return request.Task;
        }

        public INmsMessageFactory MessageFactory => provider.MessageFactory;

        public long SendTimeout => sendTimeout;

        public Task Send(OutboundMessageDispatch envelope)
        {
            CheckClosed();

            FailoverRequest request = new FailoverRequest(this, SendTimeout)
            {
                DoTask = activeProvider => activeProvider.Send(envelope),
                Name = nameof(Send)
            };

            request.Run();

            return request.Task;
        }

        public Task Unsubscribe(string name)
        {
            CheckClosed();

            FailoverRequest request = new FailoverRequest(this, SendTimeout)
            {
                DoTask = activeProvider => activeProvider.Unsubscribe(name),
                Name = nameof(Unsubscribe)
            };

            request.Run();

            return request.Task;
        }

        public Task Rollback(NmsTransactionInfo transactionInfo, NmsTransactionInfo nextTransactionInfo)
        {
            CheckClosed();

            FailoverRequest request = new FailoverRequest(this, SendTimeout)
            {
                DoTask = activeProvider => activeProvider.Rollback(transactionInfo, nextTransactionInfo),
                Name = nameof(Rollback),
                SucceedsWhenOffline = true
            };

            request.Run();

            return request.Task;
        }

        public Task Commit(NmsTransactionInfo transactionInfo, NmsTransactionInfo nextTransactionInfo)
        {
            CheckClosed();

            FailoverRequest request = new FailoverRequest(this, SendTimeout)
            {
                DoTask = activeProvider => activeProvider.Commit(transactionInfo, nextTransactionInfo),
                Name = nameof(Commit),
                FailureWhenOffline = true
            };

            request.Run();

            return request.Task;
        }

        public void OnInboundMessage(InboundMessageDispatch envelope)
        {
            if (closed)
                return;

            listener.OnInboundMessage(envelope);
        }

        public void OnConnectionFailure(NMSException exception)
        {
            Tracer.Debug($"Failover: the provider reports failure: {exception.Message}");
            HandleProviderError(provider, exception);
        }

        public Task OnConnectionRecovery(IProvider provider)
        {
            return Task.CompletedTask;
        }

        public void OnConnectionEstablished(Uri remoteUri)
        {
        }

        public Task OnConnectionRecovered(IProvider provider)
        {
            return Task.CompletedTask;
        }

        public void OnConnectionRestored(Uri remoteUri)
        {
        }

        public void OnConnectionInterrupted(Uri failedUri)
        {
        }

        public void OnResourceClosed(INmsResource resourceInfo, Exception error)
        {
            if (closed)
                return;

            listener.OnResourceClosed(resourceInfo, error);
        }

        internal void HandleProviderError(IProvider provider, NMSException cause)
        {
            if (closed)
                return;

            lock (SyncRoot)
            {
                // It is possible that another failed request signaled an error for the same provider
                // and we already cleaned up the old failed provider and scheduled a reconnect that
                // has already succeeded, so we need to ensure that we don't kill a valid provider.
                if (provider == this.provider)
                {
                    Tracer.Debug($"handling Provider failure: {cause.ToString()}");
                    this.provider = null;
                    provider.SetProviderListener(null);
                    Uri failedUri = provider.RemoteUri;
                    try
                    {
                        provider.Close();
                    }
                    catch (Exception exception)
                    {
                        Tracer.Debug($"Caught exception while closing failed provider: {exception.Message}");
                    }

                    if (reconnectControl.IsReconnectAllowed(cause))
                    {
                        // Start watching for request timeouts while we are offline, unless we already are.
                        foreach (FailoverRequest failoverRequest in GetPendingRequests())
                        {
                            failoverRequest.ScheduleTimeout();
                        }

                        TriggerReconnectionAttempt();

                        listener?.OnConnectionInterrupted(failedUri);
                    }
                    else
                    {
                        listener?.OnConnectionFailure(cause);
                    }
                }
                else
                {
                    Tracer.Debug($"Ignoring duplicate provider failed event for provider: {provider}");
                }
            }
        }

        private void CheckClosed()
        {
            if (closed)
                throw new IOException("The Provider is already closed");
        }

        internal void AddFailoverRequest(FailoverRequest request)
        {
            lock (requests)
            {
                requests.Add(request);
            }
        }

        internal void RemoveFailoverRequest(FailoverRequest request)
        {
            lock (requests)
            {
                requests.Remove(request);
            }
        }

        internal IEnumerable<FailoverRequest> GetPendingRequests()
        {
            lock (requests)
            {
                return requests.ToArray();
            }
        }

        private class ReconnectControls
        {
            private readonly FailoverProvider failoverProvider;

            // Reconnection state tracking
            private volatile bool recoveryRequired;
            private long reconnectAttempts;
            private long nextReconnectDelay = -1;

            public ReconnectControls(FailoverProvider failoverProvider)
            {
                this.failoverProvider = failoverProvider;
            }

            public long ReconnectAttempts => reconnectAttempts;

            public async Task ScheduleReconnect(Func<Task> action)
            {
                // If no connection recovery required then we have never fully connected to a remote
                // so we proceed down the connect with one immediate connection attempt and then follow
                // on delayed attempts based on configuration.
                if (!recoveryRequired)
                {
                    if (ReconnectAttempts == 0)
                    {
                        Tracer.Debug("Initial connect attempt will be performed immediately");
                        await action();
                    }
                    else if (ReconnectAttempts == 1 && failoverProvider.InitialReconnectDelay > 0)
                    {
                        Tracer.Debug($"Delayed initial reconnect attempt will be in {failoverProvider.InitialReconnectDelay} milliseconds");
                        await Task.Delay(TimeSpan.FromMilliseconds(failoverProvider.InitialReconnectDelay));
                        await action();
                    }
                    else
                    {
                        double delay = NextReconnectDelay();
                        Tracer.Debug($"Next reconnect attempt will be in {delay} milliseconds");
                        await Task.Delay(TimeSpan.FromMilliseconds(delay));
                        await action();
                    }
                }
                else if (ReconnectAttempts == 0)
                {
                    if (failoverProvider.InitialReconnectDelay > 0)
                    {
                        Tracer.Debug($"Delayed initial reconnect attempt will be in {failoverProvider.InitialReconnectDelay} milliseconds");
                        await Task.Delay(TimeSpan.FromMilliseconds(failoverProvider.InitialReconnectDelay));
                        await action();
                    }
                    else
                    {
                        Tracer.Debug("Initial Reconnect attempt will be performed immediately");
                        await action();
                    }
                }
                else
                {
                    double delay = NextReconnectDelay();
                    Tracer.Debug($"Next reconnect attempt will be in {delay} milliseconds");
                    await Task.Delay(TimeSpan.FromMilliseconds(delay));
                    await action();
                }
            }

            private long NextReconnectDelay()
            {
                if (nextReconnectDelay == -1)
                {
                    nextReconnectDelay = failoverProvider.ReconnectDelay;
                }

                if (failoverProvider.UseReconnectBackOff && ReconnectAttempts > 1)
                {
                    // Exponential increment of reconnect delay.
                    nextReconnectDelay = (long) Math.Round(nextReconnectDelay * failoverProvider.ReconnectBackOffMultiplier);
                    if (nextReconnectDelay > failoverProvider.MaxReconnectDelay)
                    {
                        nextReconnectDelay = failoverProvider.MaxReconnectDelay;
                    }
                }

                return nextReconnectDelay;
            }

            public long RecordNextAttempt()
            {
                return ++reconnectAttempts;
            }

            public void ConnectionEstablished()
            {
                recoveryRequired = true;
                nextReconnectDelay = -1;
                reconnectAttempts = 0;
            }

            public bool IsReconnectAllowed(Exception cause)
            {
                // If a connection attempts fail due to Security errors than we abort
                // reconnection as there is a configuration issue and we want to avoid
                // a spinning reconnect cycle that can never complete.
                if (IsStoppageCause(cause))
                {
                    return false;
                }

                return !IsLimitExceeded();
            }

            private bool IsStoppageCause(Exception cause)
            {
                // TODO: Check if fail is due to Security errors
                return false;
            }

            public bool IsLimitExceeded()
            {
                int reconnectLimit = ReconnectAttemptLimit();
                if (reconnectLimit != UNDEFINED && reconnectAttempts >= reconnectLimit)
                {
                    return true;
                }

                return false;
            }

            private int ReconnectAttemptLimit()
            {
                int maxReconnectValue = failoverProvider.MaxReconnectAttempts;
                if (!recoveryRequired && failoverProvider.StartupMaxReconnectAttempts != UNDEFINED)
                {
                    // If this is the first connection attempt and a specific startup retry limit
                    // is configured then use it, otherwise use the main reconnect limit
                    maxReconnectValue = failoverProvider.StartupMaxReconnectAttempts;
                }

                return maxReconnectValue;
            }

            public bool IsRecoveryRequired()
            {
                return recoveryRequired;
            }
        }
    }
}