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

using Apache.NMS.AMQP.Util;
using NUnit.Framework;

namespace NMS.AMQP.Test
{
    [TestFixture]
    public class AtomicBoolTest
    {
        [Test]
        public void TestCompareAndSetAtomicBoolToTrue()
        {
            AtomicBool atomicBool = new AtomicBool();
            Assert.False(atomicBool.Value);
            Assert.True(atomicBool.CompareAndSet(false, true));
            Assert.True(atomicBool.Value);
        }

        [Test]
        public void TestCompareAndSetAtomicBoolToFalse()
        {
            AtomicBool atomicBool = new AtomicBool(true);
            Assert.True(atomicBool.Value);
            Assert.True(atomicBool.CompareAndSet(true, false));
            Assert.False(atomicBool.Value);
        }

        [Test]
        public void TestCompareAndSetNotSetAtomicBoolWhenActualValueWasNotEqualToTheExpectedValue()
        {
            AtomicBool atomicBool = new AtomicBool();
            Assert.False(atomicBool.Value);
            Assert.False(atomicBool.CompareAndSet(true, false));
        }

        [Test]
        public void TestSetValueToTrue()
        {
            AtomicBool atomicBool = new AtomicBool();
            Assert.False(atomicBool.Value);
            atomicBool.Set(true);
            Assert.True(atomicBool.Value);
        }

        [Test]
        public void TestSetValueToFalse()
        {
            AtomicBool atomicBool = new AtomicBool(true);
            Assert.True(atomicBool.Value);
            atomicBool.Set(false);
            Assert.False(atomicBool.Value);
        }
    }
}