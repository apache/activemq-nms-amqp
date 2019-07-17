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
using System.Collections.Specialized;
using Apache.NMS.AMQP.Util;
using NUnit.Framework;

namespace NMS.AMQP.Test
{
    [TestFixture]
    public class PropertyUtilTest
    {
        [Test]
        public void TestFilterPropertiesNullProperties()
        {
            Assert.Throws<ArgumentNullException>(() => PropertyUtil.FilterProperties(null, "option."));
        }

        [Test]
        public void TestFilterPropertiesNoFilter()
        {
            StringDictionary properties = new StringDictionary();
            properties.Add("unprefixied", "true");
            properties.Add("option.filtered1", "true");
            properties.Add("option.filtered2", "false");

            StringDictionary result = PropertyUtil.FilterProperties(properties, "option.");
            Assert.AreEqual(2, result.Count);
            Assert.True(result.ContainsKey("filtered1"));
            Assert.True(result.ContainsKey("filtered2"));
            Assert.False(result.ContainsKey("unprefixed"));
        }

        [Test]
        public void TestSetProperties()
        {
            Options options = new Options();
            var properties = new StringDictionary
            {
                {"firstName", "foo"},
                {"lastName", "bar"},
                {"numberValue", "5"},
                {"booleanValue", "true"},

            };

            PropertyUtil.SetProperties(options, properties);

            Assert.AreEqual("foo", options.FirstName);
            Assert.AreEqual("bar", options.LastName);
            Assert.AreEqual(5, options.NumberValue);
            Assert.AreEqual(true, options.BooleanValue);
        }

        private class Options
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public int NumberValue { get; set; }
            public bool BooleanValue { get; set; }
        }
    }
}