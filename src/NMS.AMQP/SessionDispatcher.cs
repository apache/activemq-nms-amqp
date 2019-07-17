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

using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Apache.NMS.AMQP
{
    internal class SessionDispatcher
    {
        private readonly ActionBlock<NmsMessageConsumer.MessageDeliveryTask> actionBlock;
        private int dispatchThreadId;
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

        public bool IsOnDeliveryThread() => dispatchThreadId == Thread.CurrentThread.ManagedThreadId;

        private void HandleTask(NmsMessageConsumer.MessageDeliveryTask messageDeliveryTask)
        {
            try
            {
                dispatchThreadId = Thread.CurrentThread.ManagedThreadId;
                messageDeliveryTask.DeliverNextPending();
            }
            finally
            {
                dispatchThreadId = -1;
            }
        }

        public void Stop()
        {
            actionBlock.Complete();
            cts.Cancel();
            cts.Dispose();
        }
    }
}