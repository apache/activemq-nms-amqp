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

using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Apache.NMS.AMQP.Util.Synchronization
{
    public static class TaskExtensions
    {
        
        public static T GetAsyncResult<T>(this Task<T> task)
        {
            return task.Await().GetAwaiter().GetResult();
        }
        
        public static void GetAsyncResult(this Task task)
        {
            task.Await().GetAwaiter().GetResult();
        }

        public static async Task AwaitRunContinuationAsync(this Task task)
        {
            await task.Await();
            if (TaskSynchronizationSettings.TryToRunCertainContunuationsAsynchronously)
            {
                await Task.Yield();
            }
        }
        
        public static async Task<T> AwaitRunContinuationAsync<T>(this Task<T> task)
        {
            T t = await task.Await();
            if (TaskSynchronizationSettings.TryToRunCertainContunuationsAsynchronously)
            {
                await Task.Yield();
            }
            return t;
        }
        
        public static ConfiguredTaskAwaitable Await(this Task task)
        {
            return task.ConfigureAwait(TaskSynchronizationSettings.ContinueOnCapturedContext);
        }
        
        public static ConfiguredTaskAwaitable<T> Await<T>(this Task<T> task)
        {
            return task.ConfigureAwait(TaskSynchronizationSettings.ContinueOnCapturedContext);
        }
        
    }
}