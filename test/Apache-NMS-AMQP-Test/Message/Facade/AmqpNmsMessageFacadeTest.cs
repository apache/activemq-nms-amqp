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