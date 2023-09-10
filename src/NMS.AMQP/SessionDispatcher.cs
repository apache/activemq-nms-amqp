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
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Apache.NMS.AMQP.Util.Synchronization;

namespace Apache.NMS.AMQP
{
    internal class SessionDispatcher
    {
        private readonly ActionBlock<NmsMessageConsumer.MessageDeliveryTask> actionBlock;
        private readonly AsyncLocal<bool> isOnDispatcherFlow = new AsyncLocal<bool>();
        private readonly CancellationTokenSource cts;

        public SessionDispatcher()
        {
            cts = new CancellationTokenSource();
            actionBlock = new ActionBlock<NmsMessageConsumer.MessageDeliveryTask>(HandleTask, new ExecutionDataflowBlockOptions
            {
                EnsureOrdered = true,
                MaxDegreeOfParallelism = 1,
                CancellationToken = cts.Token
            });
        }

        public void Post(NmsMessageConsumer.MessageDeliveryTask task) => actionBlock.Post(task);

        public bool IsOnDeliveryExecutionFlow() => isOnDispatcherFlow.Value;

        private async Task HandleTask(NmsMessageConsumer.MessageDeliveryTask messageDeliveryTask)
        {
            try
            {
                isOnDispatcherFlow.Value = true;
                await messageDeliveryTask.DeliverNextPending(cts.Token).Await();
            }
            finally
            {
                isOnDispatcherFlow.Value = false;
            }
        }

        public void Stop()
        {
            actionBlock.Complete();
            cts.Cancel();
            cts.Dispose();
        }

        public IDisposable ExcludeCheckIsOnDeliveryExecutionFlow()
        {
            return new ExcludeCheckIsOnDeliveryExecutionFlowBlock(this);
        }

        private class ExcludeCheckIsOnDeliveryExecutionFlowBlock : IDisposable
        {
            private readonly bool previousValue = false;
            private readonly SessionDispatcher sessionDispatcher;

            public ExcludeCheckIsOnDeliveryExecutionFlowBlock(SessionDispatcher sessionDispatcher)
            {
                this.sessionDispatcher = sessionDispatcher;
                this.previousValue = sessionDispatcher.isOnDispatcherFlow.Value;
                sessionDispatcher.isOnDispatcherFlow.Value = false;
            }

            public void Dispose()
            {
                sessionDispatcher.isOnDispatcherFlow.Value = previousValue;
            }
        }
    }
}