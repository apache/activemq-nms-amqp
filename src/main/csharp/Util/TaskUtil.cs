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

namespace NMS.AMQP.Util
{
    class TaskUtil
    {
        public static T Wait<T>(Task<T> t, long millis)
        {
            return TaskUtil.Wait(t, TimeSpan.FromMilliseconds(millis));
        }
        public static bool Wait(Task t, long millis)
        {
            return TaskUtil.Wait(t, TimeSpan.FromMilliseconds(millis));
        }

        public static T Wait<T>(Task<T> t, TimeSpan ts)
        {
            if (TaskUtil.Wait((Task)t, ts))
            {
                if (t.Exception != null)
                {
                    return default(T);
                }
                else
                {
                    return t.Result;
                }
            }
            else
            {
                throw new NMSException(string.Format("Failed to exceute task {0} in time {1}ms.", t, ts.TotalMilliseconds));
                //return default(T);
            }
        }

        public static bool Wait(Task t, TimeSpan ts)
        {
            DateTime current = DateTime.Now;
            DateTime end = current.Add(ts);
            const int TIME_INTERVAL = 100;
            do
            {
                try
                {
                    t.Wait(TIME_INTERVAL);
                }
                catch (AggregateException ae)
                {
                    Exception ex = ae;
                    if (t.IsFaulted || t.IsCanceled || t.Exception != null)
                    {
                        Tracer.DebugFormat("Error Excuting task Failed to Complete: {0}", t.Exception);
                        break;
                    }
                }
            } while ((current = current.AddMilliseconds(TIME_INTERVAL)) < end && !t.IsCompleted);
            return t.IsCompleted;
        }
    }
}
