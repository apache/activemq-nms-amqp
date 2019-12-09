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

using System.Linq;
using Apache.NMS.AMQP.Util;
using NUnit.Framework;

namespace NMS.AMQP.Test.Utils
{
    [TestFixture]
    public class IdGeneratorTest
    {
        [Test]
        public void TestSanitizeHostName()
        {
            Assert.AreEqual("somehost.lan", IdGenerator.SanitizeHostName("somehost.lan"));

            // include a UTF-8 char in the text \u0E08 is a Thai elephant
            Assert.AreEqual("otherhost.lan", IdGenerator.SanitizeHostName("other\u0E08host.lan"));
        }

        [Test]
        public void TestDefaultPrefix()
        {
            var generator = new IdGenerator();
            string generated = generator.GenerateId();
            Assert.True(generated.StartsWith(IdGenerator.DEFAULT_PREFIX));
            Assert.IsFalse(generated.Substring(IdGenerator.DEFAULT_PREFIX.Length).StartsWith(":"));
        }

        [Test]
        public void TestNonDefaultPrefix()
        {
            var idGenerator = new IdGenerator("TEST-");
            var generated = idGenerator.GenerateId();
            Assert.IsFalse(generated.StartsWith(IdGenerator.DEFAULT_PREFIX));
            Assert.IsFalse(generated.Substring("TEST-".Length).StartsWith(":"));
        }

        [Test]
        public void TestIdIndexIncrements()
        {
            var generator = new IdGenerator();
            var sequences = Enumerable.Repeat(0, 5)
                .Select(x => generator.GenerateId())
                .Select(x => x.Split(':').Last())
                .Select(int.Parse);

            CollectionAssert.AreEqual(new[] { 1, 2, 3, 4, 5 }, sequences);
        }
    }
}