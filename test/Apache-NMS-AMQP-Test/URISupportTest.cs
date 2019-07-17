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
using Apache.NMS.AMQP.Util;
using NUnit.Framework;

namespace NMS.AMQP.Test
{
    // TODO: Move to NMS API
    [TestFixture]
    public class URISupportTest
    {
        [Test]
        public void TestIsCompositeURIWithQueryNoSlashes()
        {
            Uri[] compositeUrIs = { new Uri("test:(part1://host?part1=true)?outside=true"), new Uri("broker:(tcp://localhost:61616)?name=foo") };
            foreach (Uri uri in compositeUrIs)
            {
                Assert.True(URISupport.IsCompositeUri(uri), uri + "must be detected as composite URI");
            }
        }

        [Test]
        public void TestIsCompositeURINoQueryNoSlashes()
        {
            Uri[] compositeUrIs = new Uri[] { new Uri("test:(part1://host,part2://(sub1://part,sube2:part))"), new Uri("test:(path)/path") };
            foreach (Uri uri in compositeUrIs)
            {
                Assert.True(URISupport.IsCompositeUri(uri), uri + "must be detected as composite URI");
            }
        }

        [Test]
        public void TestIsCompositeWhenURIHasUnmatchedParends()
        {
            Uri uri = new Uri("test:(part1://host,part2://(sub1://part,sube2:part)");
            Assert.False(URISupport.IsCompositeUri(uri));
        }

        [Test]
        public void TestIsCompositeURINoQueryNoSlashesNoParentheses()
        {
            Assert.False(URISupport.IsCompositeUri(new Uri("test:part1")), "test:part1" + " must be detected as non-composite URI");
        }
    }
}