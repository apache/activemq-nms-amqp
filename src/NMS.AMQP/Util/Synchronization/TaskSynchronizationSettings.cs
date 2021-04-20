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

namespace Apache.NMS.AMQP.Util.Synchronization
{
    
    public static class TaskSynchronizationSettings
    {
        /// <summary>
        /// General settings for awaits, whether to force continuation on captured context
        /// </summary>
        public static bool ContinueOnCapturedContext { get; set; } = false;

        /// <summary>
        /// In AMQP library there is a async pump, that is calling methods, and in turn continuation may be inlined, if this happens
        /// and continuation is waiting synchronously on something, then it stops processing in that pump, and it may lead to deadlocks
        /// if continuation is waiting on something (some message) that async pump was supposed to deliver-but now async pump stopped processing
        /// It seems it was also discussed here https://github.com/Azure/amqpnetlite/issues/237
        /// So by default there is a try to force continuation from different thread (by using) yield, on those tasks that it seems that their
        /// continuation may be from async pump
        /// </summary>
        public static bool TryToRunCertainContunuationsAsynchronously { get; set; } = true;
    }
}