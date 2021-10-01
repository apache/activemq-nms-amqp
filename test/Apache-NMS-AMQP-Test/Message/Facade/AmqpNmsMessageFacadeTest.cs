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
using Apache.NMS.AMQP.Message;
using Apache.NMS.AMQP.Provider.Amqp;
using Apache.NMS.AMQP.Provider.Amqp.Message;
using Apache.NMS.AMQP.Util;
using Apache.NMS.Util;
using Moq;
using NUnit.Framework;

namespace NMS.AMQP.Test.Message.Facade
{
    [TestFixture]
    public class AmqpNmsMessageFacadeTest
    {
        [Test]
        public void TestNMSDeliveryTime_Set_ShouldHaveMessageAnnotations()
        {
            AmqpNmsMessageFacade msg = new AmqpNmsMessageFacade();
            
            Mock<IAmqpConnection> mockAmqpConnection = new Mock<IAmqpConnection>();
            
            msg.Initialize(mockAmqpConnection.Object);
            var deliveryTime = DateTime.UtcNow.AddMinutes(2);
            msg.DeliveryTime = deliveryTime;

            Assert.AreEqual(new DateTimeOffset(deliveryTime).ToUnixTimeMilliseconds(), msg.MessageAnnotations[SymbolUtil.NMS_DELIVERY_TIME]);
        }
    }
}