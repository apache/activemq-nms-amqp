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
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS.AMQP.Util.Synchronization;
using NUnit.Framework;

namespace NMS.AMQP.Test.Utils.Synchronization
{
    [TestFixture]
    public class NmsSynchronizationMonitorTest
    {
        public class EventList
        {
            public List<string> list = new List<string>();
            public Dictionary<string, List<string>> listsByPrefix = new Dictionary<string, List<string>>();

            public void Add(string ev)
            {
                lock (this)
                {
                    list.Add(ev);
                    string prefix = new string(new[] {ev[0]});
                    if (!listsByPrefix.ContainsKey(prefix))
                    {
                        listsByPrefix[prefix] = new List<string>();
                    }
                    listsByPrefix[prefix].Add(ev);
                }
            }

            public string ToString(string prefix)
            {
                return listsByPrefix.ContainsKey(prefix) ? string.Join("",listsByPrefix[prefix]) : string.Empty;
            }
            
            public override string ToString()
            {
                return string.Join("", list);
            }
        }


        [Test]
        public void TestNestedLockAndWait()
        {
            NmsSynchronizationMonitor syncRoot = new NmsSynchronizationMonitor();
            ManualResetEvent lockedEvent = new ManualResetEvent(false);

            EventList evList = new EventList();

            var task = Task.Run(() =>
            {
                Thread.Sleep(5);
                using (syncRoot.Lock())
                {
                    evList.Add("A1");
                    Thread.Sleep(1);
                    evList.Add("A1");
                    Thread.Sleep(1);
                    evList.Add("A1");
                    Thread.Sleep(1);

                    using (syncRoot.Lock())
                    {
                        evList.Add("A1");
                        Thread.Sleep(1);
                        evList.Add("A1");
                        Thread.Sleep(1);
                        evList.Add("A1");
                        Thread.Sleep(1);

                        lockedEvent.Set();
                        syncRoot.Wait();

                        evList.Add("A2");
                        Thread.Sleep(1);
                        evList.Add("A2");
                        Thread.Sleep(1);
                        evList.Add("A2");
                        Thread.Sleep(1);
                    }

                    evList.Add("A2");
                    Thread.Sleep(1);
                    evList.Add("A2");
                    Thread.Sleep(1);
                    evList.Add("A2");
                    Thread.Sleep(1);
                }
            });

            var taskB = Task.Run(() =>
            {
                while (!task.IsCompleted)
                {
                    using (syncRoot.Lock())
                    {
                        evList.Add("B");
                        Thread.Sleep(1);
                    }
                }
            });

            lockedEvent.WaitOne();
            Thread.Sleep(10); // to give Task A Time to go to sleep and B to work


            Task.Run(() => { syncRoot.Pulse(); });

            task.Wait();
            taskB.Wait();


            // Now Asses that block A1 and A2 are not intersected by B, however A1 And A2 should have some B between (during sleep period)
            var events = evList.list.Select((a, i) => new Tuple<string, int>(a, i));
            var a1 = events.Where(a => a.Item1 == "A1").ToList();
            var a2 = events.Where(a => a.Item1 == "A2").ToList();
            Assert.AreEqual(6, a1.Count());
            Assert.AreEqual(5, a1.Last().Item2 - a1.First().Item2); // not intersected by anything

            Assert.AreEqual(6, a2.Count());
            Assert.AreEqual(5, a2.Last().Item2 - a2.First().Item2); // not intersected by anything

            Assert.Greater(Math.Abs(a1.Last().Item2 - a2.First().Item2), 1, "A1 and A2, should be intersected by B, and not happening one right after another");
        }

        [Test]
        public void TestNestedLockAndWaitAsync()
        {
            NmsSynchronizationMonitor syncRoot = new NmsSynchronizationMonitor();
            ManualResetEvent lockedEvent = new ManualResetEvent(false);

            EventList evList = new EventList();

            var task = Task.Run(async () =>
            {
                Thread.Sleep(5);
                using (await syncRoot.LockAsync())
                {
                    evList.Add("A1");
                    await Task.Delay(1);
                    await Task.Yield();
                    evList.Add("A1");
                    await Task.Delay(1);
                    await Task.Yield();
                    evList.Add("A1");
                    await Task.Delay(1);
                    await Task.Yield();

                    using (await syncRoot.LockAsync())
                    {
                        evList.Add("A1");
                        await Task.Delay(1);
                        await Task.Yield();
                        evList.Add("A1");
                        await Task.Delay(1);
                        await Task.Yield();
                        evList.Add("A1");
                        await Task.Delay(1);
                        await Task.Yield();

                        lockedEvent.Set();
                        await syncRoot.WaitAsync();

                        evList.Add("A2");
                        await Task.Delay(1);
                        await Task.Yield();
                        evList.Add("A2");
                        await Task.Delay(1);
                        await Task.Yield();
                        evList.Add("A2");
                        await Task.Delay(1);
                        await Task.Yield();
                    }

                    evList.Add("A2");
                    await Task.Delay(1);
                    await Task.Yield();
                    evList.Add("A2");
                    await Task.Delay(1);
                    await Task.Yield();
                    evList.Add("A2");
                    await Task.Delay(1);
                    await Task.Yield();
                }
            });

            var taskB = Task.Run(async () =>
            {
                while (!task.IsCompleted)
                {
                    using (await syncRoot.LockAsync())
                    {
                        evList.Add("B");
                        await Task.Delay(1);
                    }
                }
            });

            lockedEvent.WaitOne();
            Thread.Sleep(10); // to give Task A Time to go to sleep and B to work


            Task.Run(() => { syncRoot.Pulse(); });

            task.Wait();
            taskB.Wait();


            // Now Asses that block A1 and A2 are not intersected by B, however A1 And A2 should have some B between (during sleep period)
            var events = evList.list.Select((a, i) => new Tuple<string, int>(a, i));
            var a1 = events.Where(a => a.Item1 == "A1").ToList();
            var a2 = events.Where(a => a.Item1 == "A2").ToList();
            Assert.AreEqual(6, a1.Count());
            Assert.AreEqual(5, a1.Last().Item2 - a1.First().Item2); // not intersected by anything

            Assert.AreEqual(6, a2.Count());
            Assert.AreEqual(5, a2.Last().Item2 - a2.First().Item2); // not intersected by anything

            Assert.Greater(Math.Abs(a1.Last().Item2 - a2.First().Item2), 1, "A1 and A2, should be intersected by B, and not happening one right after another");
        }
        
        [TestCase(0.25,1000,6)]
        [TestCase(0,1000,200)]
        [Timeout(20_000)]
        public void TestConcurrentProducersSyncAndAsync(double sleepTimeMs, int minTestTimeMs, int minimumOccurences)
        {
            TimeSpan sleepTime = TimeSpan.FromMilliseconds(sleepTimeMs);
            
            EventList evListCommon = new EventList();
            
            NmsSynchronizationMonitor syncRootA = new NmsSynchronizationMonitor();
            NmsSynchronizationMonitor syncRootB = new NmsSynchronizationMonitor();
            bool runTest = true;

            // Counter helping us signal situation when we can stop running tasks/threads
            Dictionary<String, CountdownEventEx> counters = new Dictionary<string, CountdownEventEx>()
            {
                ["A1"] = new CountdownEventEx((int) Math.Ceiling(minimumOccurences / 3.0)),
                ["B1"] = new CountdownEventEx((int) Math.Ceiling(minimumOccurences / 3.0)),
                ["A2"] = new CountdownEventEx((int) Math.Ceiling(minimumOccurences / 3.0)),
                ["B2"] = new CountdownEventEx((int) Math.Ceiling(minimumOccurences / 3.0)),
                ["A3"] = new CountdownEventEx((int) Math.Ceiling(minimumOccurences / 3.0)),
                ["B3"] = new CountdownEventEx((int) Math.Ceiling(minimumOccurences / 3.0)),
            };
            
            var task1 = Task.Run(async () =>
            {
                int counter = 0;
                while (runTest)
                {
                    var lockA = (counter % 2 == 0) ? syncRootA.Lock() : await syncRootA.LockAsync();
                    using (lockA)
                    {
                        await Task.Delay(sleepTime);
                        await Task.Yield();
                        evListCommon.Add("A1");
                        await Task.Delay(sleepTime);
                        await Task.Yield();
                        evListCommon.Add("A1");
                        await Task.Delay(sleepTime);
                        await Task.Yield();
                        evListCommon.Add("A1");
                        counters["A1"].TrySignal();
                        await Task.Delay(sleepTime);
                        await Task.Yield();

                        var lockB = (counter % 2 == 0) ? syncRootB.Lock() : await syncRootB.LockAsync();
                        using (lockB)
                        {
                            evListCommon.Add("B1");
                            await Task.Delay(sleepTime);
                            await Task.Yield();
                            evListCommon.Add("B1");
                            await Task.Delay(sleepTime);
                            await Task.Yield();
                            evListCommon.Add("B1");
                            counters["B1"].TrySignal();
                            await Task.Delay(sleepTime);
                            await Task.Yield();
                        }
                        
                        await Task.Delay(sleepTime);
                        await Task.Yield();
                        evListCommon.Add("A1");
                        await Task.Delay(sleepTime);
                        await Task.Yield();
                        evListCommon.Add("A1");
                        await Task.Delay(sleepTime);
                        await Task.Yield();
                        evListCommon.Add("A1");
                        counters["A1"].TrySignal();
                        await Task.Delay(sleepTime);
                        await Task.Yield();
                    }

                    counter++;
                }
            });

            var task2 = Task.Run(async () =>
            {
                int counter = 0;

                // Also test the reentrancy of lock and mix of async and non async
                async Task ResourceAccess(string symbol, int level, NmsSynchronizationMonitor syncRoot)
                {
                    var locked = (counter % 2 == 0) ? syncRoot.Lock() : await syncRoot.LockAsync();
                    using (locked)
                    {
                        int depth = counter % 4;
                        if (level == depth)
                        {
                            await Task.Delay(sleepTime);
                            await Task.Yield();
                            evListCommon.Add(symbol);
                            await Task.Delay(sleepTime);
                            await Task.Yield();
                            evListCommon.Add(symbol);
                            await Task.Delay(sleepTime);
                            await Task.Yield();
                            evListCommon.Add(symbol);
                            counters[symbol].TrySignal();
                            await Task.Delay(sleepTime);
                            await Task.Yield();
                        }
                        else
                        {
                            await ResourceAccess(symbol, level + 1, syncRoot);
                        }
                    }
                } 
                
                while (runTest)
                {
                    await ResourceAccess("A2",0, syncRootA);
                    await ResourceAccess("B2",0, syncRootB);
                    
                    counter++;
                }
            });
            
            var task3 = new Thread(() =>
            {
                int counter = 0;
                while (runTest)
                {
                    var lockA = (counter % 2 == 0) ? syncRootA.Lock() : syncRootA.LockAsync().GetAsyncResult();
                    using (lockA)
                    {
                        Thread.Sleep(sleepTime);
                        evListCommon.Add("A3");
                        Thread.Sleep(sleepTime);
                        evListCommon.Add("A3");
                        Thread.Sleep(sleepTime);
                        evListCommon.Add("A3");
                        counters["A3"].TrySignal();
                        Thread.Sleep(sleepTime);
                    }
                    
                    var lockB = (counter % 2 == 0) ? syncRootB.Lock() : syncRootB.LockAsync().GetAsyncResult();
                    using (lockB)
                    {
                        evListCommon.Add("B3");
                        Thread.Sleep(sleepTime);
                        evListCommon.Add("B3");
                        Thread.Sleep(sleepTime);
                        evListCommon.Add("B3");
                        counters["B3"].TrySignal();
                        Thread.Sleep(sleepTime);
                    }

                    counter++;
                }
            });
            task3.IsBackground = true;
            task3.Start();
            
            // Let it run for a short while
            Thread.Sleep(minTestTimeMs);
            
            // Wait for all counter events to get to zero so min occurences should be met
            counters.ToList().ForEach(counterEvent => counterEvent.Value.Wait());
                        
            runTest = false;
            task1.Wait();
            task2.Wait();
            task3.Join();

            var sequenceCommon = evListCommon.ToString();
            var sequenceA = evListCommon.ToString("A");
            var sequenceB = evListCommon.ToString("B");
            
            // remove B locks interfering with A sequence of validating task1
            Enumerable.Range(0,10).ToList().ForEach( (i) => sequenceCommon = sequenceCommon
                .Replace("A1B2", "A1")
                .Replace("A1B3", "A1")
                .Replace("B2A1","A1")
                .Replace("B3A1","A1")
            );
            sequenceCommon = sequenceCommon.Replace("A1A1A1B1B1B1A1A1A1", "");
            // The only allowed sequence in common for task1 is nested sequence
            Assert.IsFalse(sequenceCommon.Contains("A1"), "Sequence should only contain task 1 resource in right order:"+sequenceCommon);
            Assert.IsFalse(sequenceCommon.Contains("B1"), "Sequence should only contain task 1 resource in right order:"+sequenceCommon);

            int countA1 = sequenceA.Where(a => a == '1').Count();
            int countA2 = sequenceA.Where(a => a == '2').Count();
            int countA3 = sequenceA.Where(a => a == '3').Count();
            
            int countB1 = sequenceB.Where(a => a == '1').Count();
            int countB2 = sequenceB.Where(a => a == '2').Count();
            int countB3 = sequenceB.Where(a => a == '3').Count();

            // Assert that all of the threads had their fair share of action
            Assert.GreaterOrEqual(countA1, minimumOccurences);
            Assert.GreaterOrEqual(countA2, minimumOccurences);
            Assert.GreaterOrEqual(countA3, minimumOccurences);
            Assert.GreaterOrEqual(countB1, minimumOccurences);
            Assert.GreaterOrEqual(countB2, minimumOccurences);
            Assert.GreaterOrEqual(countB3, minimumOccurences);
            
            
            sequenceA = sequenceA.Replace("A1A1A1", "");
            sequenceA = sequenceA.Replace("A2A2A2", "");
            sequenceA = sequenceA.Replace("A3A3A3", "");
            
            sequenceB = sequenceB.Replace("B1B1B1", "");
            sequenceB = sequenceB.Replace("B2B2B2", "");
            sequenceB = sequenceB.Replace("B3B3B3", "");

            
            Assert.AreEqual(0, sequenceA.Length, "There were illegal sequences of execution for resource A: " + sequenceA);
            Assert.AreEqual(0, sequenceB.Length, "There were illegal sequences of execution for resource B: " + sequenceB);
            
        }

        /// <summary>
        /// Extended class to have signaling method that does not throw if count gets to minus
        /// </summary>
        private class CountdownEventEx : CountdownEvent
        {
            public CountdownEventEx(int initialCount) : base(initialCount)
            {
            }

            public bool TrySignal()
            {
                try
                {
                    this.Signal();
                    return true;
                }
                catch (Exception)
                {
                    // empty
                }

                return false;
            }
        }
    }
}