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
using Apache.NMS.AMQP;
using Apache.NMS.AMQP.Policies;
using NUnit.Framework;

namespace NMS.AMQP.Test.Policies
{
    [TestFixture]
    public class NmsDefaultDeserializationPolicyTest
    {
        [Test]
        public void TestIsTrustedType()
        {
            var destination = new NmsQueue("test-queue");
            var policy = new NmsDefaultDeserializationPolicy();
            
            Assert.False(policy.IsTrustedType(destination, null));
            Assert.True(policy.IsTrustedType(destination, typeof(Guid)));
            Assert.True(policy.IsTrustedType(destination, typeof(string)));
            Assert.True(policy.IsTrustedType(destination, typeof(bool)));
            Assert.True(policy.IsTrustedType(destination, typeof(double)));
            Assert.True(policy.IsTrustedType(destination, typeof(object)));
            
            // Only types in System
            policy.AllowList = "System";
            Assert.False(policy.IsTrustedType(destination, null));
            Assert.True(policy.IsTrustedType(destination, typeof(Guid)));
            Assert.True(policy.IsTrustedType(destination, typeof(string)));
            Assert.True(policy.IsTrustedType(destination, typeof(bool)));
            Assert.True(policy.IsTrustedType(destination, typeof(double)));
            Assert.True(policy.IsTrustedType(destination, typeof(object)));
            Assert.False(policy.IsTrustedType(destination, GetType()));
            
            // Entry must be complete namespace name prefix to match
            // i.e. while "System.C" is a prefix of "System.Collections", this
            // won't match the Queue class below.
            policy.AllowList = "System.C";
            Assert.False(policy.IsTrustedType(destination, typeof(Guid)));
            Assert.False(policy.IsTrustedType(destination, typeof(string)));
            Assert.False(policy.IsTrustedType(destination, typeof(System.Collections.Queue)));

            // Add a non-core namespace
            policy.AllowList = $"System,{GetType().Namespace}";
            Assert.True(policy.IsTrustedType(destination, typeof(string)));
            Assert.True(policy.IsTrustedType(destination, GetType()));
            
            // Try with a type-specific entry
            policy.AllowList = typeof(string).FullName;
            Assert.True(policy.IsTrustedType(destination, typeof(string)));
            Assert.False(policy.IsTrustedType(destination, typeof(bool)));
            
            // Verify deny list overrides allow list
            policy.AllowList = "System";
            policy.DenyList = "System";
            Assert.False(policy.IsTrustedType(destination, typeof(string)));
            
            // Verify deny list entry prefix overrides allow list
            policy.AllowList = typeof(string).FullName;
            policy.DenyList = typeof(string).Namespace;
            Assert.False(policy.IsTrustedType(destination, typeof(string)));
            
            // Verify deny list catch-all overrides allow list
            policy.AllowList = typeof(string).FullName;
            policy.DenyList = NmsDefaultDeserializationPolicy.CATCH_ALL_WILDCARD;
            Assert.False(policy.IsTrustedType(destination, typeof(string)));
        }

        [Test]
        public void TestNmsDefaultDeserializationPolicy()
        {
            var policy = new NmsDefaultDeserializationPolicy();
            
            Assert.IsNotEmpty(policy.AllowList);
            Assert.IsEmpty(policy.DenyList);
        }

        [Test]
        public void TestNmsDefaultDeserializationPolicyClone()
        {
            var policy = new NmsDefaultDeserializationPolicy
            {
                AllowList = "a.b.c",
                DenyList = "d.e.f"
            };

            var clone = (NmsDefaultDeserializationPolicy) policy.Clone();
            Assert.AreEqual(policy.AllowList, clone.AllowList);
            Assert.AreEqual(policy.DenyList, clone.DenyList);
            Assert.AreNotSame(clone, policy);
        }

        [Test]
        public void TestSetAllowList()
        {
            var policy = new NmsDefaultDeserializationPolicy();
            Assert.NotNull(policy.AllowList);

            policy.AllowList = null;
            Assert.NotNull(policy.AllowList);
            Assert.IsEmpty(policy.AllowList);

            policy.AllowList = string.Empty;
            Assert.NotNull(policy.AllowList);
            Assert.IsEmpty(policy.AllowList);
            
            policy.AllowList = "*";
            Assert.NotNull(policy.AllowList);
            Assert.IsNotEmpty(policy.AllowList);
            
            policy.AllowList = "a,b,c";
            Assert.NotNull(policy.AllowList);
            Assert.IsNotEmpty(policy.AllowList);
            Assert.AreEqual("a,b,c", policy.AllowList);
        }
        
        [Test]
        public void TestSetDenyList()
        {
            var policy = new NmsDefaultDeserializationPolicy();
            Assert.NotNull(policy.DenyList);

            policy.DenyList = null;
            Assert.NotNull(policy.DenyList);
            Assert.IsEmpty(policy.DenyList);

            policy.DenyList = string.Empty;
            Assert.NotNull(policy.DenyList);
            Assert.IsEmpty(policy.DenyList);
            
            policy.DenyList = "*";
            Assert.NotNull(policy.DenyList);
            Assert.IsNotEmpty(policy.DenyList);
            
            policy.DenyList = "a,b,c";
            Assert.NotNull(policy.DenyList);
            Assert.IsNotEmpty(policy.DenyList);
            Assert.AreEqual("a,b,c", policy.DenyList);
        }
    }
}