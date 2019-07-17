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
using Apache.NMS.AMQP.Provider.Failover;
using NUnit.Framework;

namespace NMS.AMQP.Test.Provider
{
    [TestFixture]
    public class FailoverUriPoolTest
    {
        private List<Uri> uris;

        [SetUp]
        public void SetUp()
        {
            uris = new List<Uri>
            {
                new Uri("tcp://192.168.2.1:5672"),
                new Uri("tcp://192.168.2.2:5672"),
                new Uri("tcp://192.168.2.3:5672"),
                new Uri("tcp://192.168.2.4:5672")
            };
        }

        [Test]
        public void TestCreateEmptyPool()
        {
            FailoverUriPool pool = new FailoverUriPool();
            Assert.AreEqual(FailoverUriPool.DEFAULT_RANDOMIZE_ENABLED, pool.IsRandomize);
        }

        [Test]
        public void TestGetNextFromEmptyPool()
        {
            FailoverUriPool pool = new FailoverUriPool();
            Assert.IsNull(pool.GetNext());
        }

        [Test]
        public void TestGetNextFromSingleValuePool()
        {
            FailoverUriPool pool = new FailoverUriPool(uris.Take(1));

            Assert.AreEqual(uris[0], pool.GetNext());
            Assert.AreEqual(uris[0], pool.GetNext());
            Assert.AreEqual(uris[0], pool.GetNext());
        }

        [Test]
        public void TestGetNextFromPool()
        {
            FailoverUriPool pool = new FailoverUriPool(uris);

            Assert.AreEqual(uris[0], pool.GetNext());
            Assert.AreEqual(uris[1], pool.GetNext());
            Assert.AreEqual(uris[2], pool.GetNext());
        }
    }
}