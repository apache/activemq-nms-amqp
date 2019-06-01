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

using Apache.NMS.AMQP.Provider.Amqp;
using NUnit.Framework;

namespace NMS.AMQP.Test.Provider.Amqp
{
    [TestFixture]
    public class AmqpMessageIdHelperTest
    {
        [Test]
        public void HasMessageIdPrefix()
        {
            string myId = "ID:something";
            Assert.True(AmqpMessageIdHelper.HasMessageIdPrefix(myId), "'ID:' prefix should have been identified");
        }

        [Test]
        public void TestHasIdPrefixWithIdButNoColonPrefix()
        {
            string myIdNoColon = "IDsomething";
            Assert.False(AmqpMessageIdHelper.HasMessageIdPrefix(myIdNoColon), "'ID' prefix should not have been identified without trailing colon");
        }

        [Test]
        public void TestHasIdPrefixWithNull()
        {
            string nullString = null;
            Assert.False(AmqpMessageIdHelper.HasMessageIdPrefix(nullString), "null string should not result in identification as having the prefix");
        }

        [Test]
        public void TestHasIdPrefixWithoutPrefix()
        {
            string myNonId = "";
            Assert.False(AmqpMessageIdHelper.HasMessageIdPrefix(myNonId), "string without 'ID:' anywhere should not have been identified as having the prefix");
        }

        [Test]
        public void TestHasIdPrefixWithLowercaseId()
        {
            string myLowerCaseNonId = "id:something";
            Assert.False(AmqpMessageIdHelper.HasMessageIdPrefix(myLowerCaseNonId), "lowercase 'id:' prefix should not result in identification as having 'ID:' prefix");
        }
    }
}