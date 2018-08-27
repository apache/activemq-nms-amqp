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
using System.Text;
using System.Collections;
using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using Apache.NMS;
using Apache.NMS.Util;
using Apache.NMS.AMQP.Test.Util;
using Apache.NMS.AMQP.Test.Attribute;

namespace Apache.NMS.AMQP.Test.TestCase
{
    [TestFixture]
    class MessageTest : BaseTestCase
    {
        internal static readonly TimeSpan TIMEOUT = TimeSpan.FromMilliseconds(30000);
        internal static readonly string MaxString;
        internal static readonly string AlmostMaxString;
        internal static readonly string String256;
        internal static readonly byte[] Bytes256;
        internal static readonly byte[] Bytes257;
        internal static readonly byte[] MaxBytes;
        internal static readonly Dictionary<string, object> LargeDictionary;
        internal static readonly List<object> LargeList;
        internal static readonly Dictionary<string, object> SmallDictionary;
        internal static readonly List<object> SmallList;
        internal static readonly object[] NMSPrimitiveTestValues = new object[]
        {
            (byte)0xef,
            48,
            false,
            'c',
            "smallString",
            "lllllllllllllongerString",
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-=_+,./<>?;':\"[]\\{}|",
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-=_+,./<>?;':\"[]\\{}|abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-=_+,./<>?;':\"[]\\{}|abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-=_+,.",
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-=_+,./<>?;':\"[]\\{}|abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-=_+,./<>?;':\"[]\\{}|abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-=_+,.?",
            1.12f,
            9223372036854775807L,
            3000000000U,
            Convert.ToByte(0x41),
            Convert.ToInt16(-95),
            55.123456789123456789,
            new byte[]{ 0xff, 0xe1, 0x1f, 0x5b, 0x8d },
        };

        // MaxFrameSize denotes the Maximum size an amqp message can be on a connection for the Message Tests.
        // 
        const int MaxFrameSize = 1024 * 1024 * 1024; // 1 GiB

        // Limit read by config file.
        static readonly int MaxDataChunkSize = (int)TestConfig.Instance.DataGeneratedChunkSize;

        // Normalized value of MaxDataChunkSize. 
        // If MaxDataChunkSize negative as int LargeLen is 0 - MaxDataChunkSize else MaxDataChunkSize
        // LargeLen has a minimum value of ushort.max .
        static readonly int LargeLen;
        // SmallLen is ten (2^10 = 1024) orders of magnitude lower then LargeLen with a minimum value of byte.MaxValue.
        static readonly int SmallLen;

        static MessageTest()
        {
            int limit = (MaxDataChunkSize < 0) ? 0 - MaxDataChunkSize : MaxDataChunkSize;
            LargeLen = Math.Max(limit, Convert.ToInt32(ushort.MaxValue) );
            SmallLen = Math.Max(LargeLen / 1024, Convert.ToInt32(byte.MaxValue));
            ulong charIndex = 0;
            ulong byteIndex = 0;
            int ValueIndex = 0;
            StringBuilder sb = new StringBuilder(LargeLen);
            LargeDictionary = new Dictionary<string, object>(LargeLen);
            LargeList = new List<object>(LargeLen);
            SmallDictionary = new Dictionary<string, object>(SmallLen);
            SmallList = new List<object>(SmallLen);
            Bytes256 = new byte[256];
            Bytes257 = new byte[257];
            MaxBytes = new byte[LargeLen];
            for(int i = 0; i< LargeLen; i++)
            {
                if(i == LargeLen - 1)
                {
                    AlmostMaxString = sb.ToString();
                }
                else if (i == 256)
                {
                    String256 = sb.ToString();
                }
                charIndex = 65 + (Convert.ToUInt64(i) % 26);
                byteIndex = Convert.ToUInt64(i) % 256;
                sb.Append(Convert.ToChar(charIndex));

                if (i < 257)
                {
                    Bytes257[i] = Convert.ToByte(byteIndex);
                    if (i < 256)
                    {
                        Bytes256[i] = Convert.ToByte(byteIndex);
                    }
                }
                MaxBytes[i] = Convert.ToByte(byteIndex);
                
                ValueIndex = i % NMSPrimitiveTestValues.Length;
                object value = NMSPrimitiveTestValues[ValueIndex];
                string key = GenerateMapKey(value, Convert.ToUInt64(i));
                LargeList.Add(value);
                LargeDictionary.Add(key, value);
                if (i < SmallLen)
                {
                    SmallList.Add(value);
                    SmallDictionary.Add(key, value);
                }
                
            }
            MaxString = sb.ToString();
            sb.Clear();
            
        }

        static string GenerateMapKey(object value, int index = 0)
        {
            return GenerateMapKey(value, Convert.ToUInt64(index));
        }
        static string GenerateMapKey(object value, ulong index = 0)
        {
            Type valueType = value.GetType();
            string valueTypeName = valueType.Name;
            if (value != null && value is byte[])
            {
                valueTypeName = "Binary";
            }
            else if (value != null && value is IList)
            {
                valueTypeName = "List";
            }
            else if (value != null && value is IDictionary)
            {
                valueTypeName = "Dictionary";
            }
            string key = string.Format("{0}Key{1}", valueTypeName, index);
            return key;
        }

        #region Message Comparision Methods

        internal static bool CompareList(IList sent, IList recv)
        {
            if (sent.Count != recv.Count)
                return false;
            for(int i=0; i<sent.Count; i++)
            {
                object sentValue = sent[i];
                object recvValue = recv[i];
                if(!Compare(sentValue, recvValue))
                {
                    return false;
                }
            }
            return true;
        }

        internal static string CompareMap(IPrimitiveMap sent, IPrimitiveMap recv)
        {
            if (sent.Count != recv.Count)
                return "Sent Map count ("+sent.Count+") != Recv Map count("+recv.Count+")";
            foreach(object key in sent.Keys)
            {
                if (!recv.Contains(key))
                {
                    return "Receive Map does not contain key: " + key;
                }
                else if (!Compare(sent[key as string], recv[key as string]))
                {
                    return "Sent (" + sent[key as string] + ") and Recv ("
                        + recv[key as string] + ") Map do not match at key:" + key;
                }
            }
            return null;
        }

        internal static bool CompareDictionary(IDictionary sent, IDictionary recv)
        {
            if (sent.Count != recv.Count)
                return false;
            IDictionaryEnumerator sentEnum = sent.GetEnumerator();
            while (sentEnum.MoveNext()  && sentEnum.Current != null)
            {
                if (!recv.Contains(sentEnum.Key))
                {
                    return false;
                }
                else if(!Compare(sentEnum.Value, recv[sentEnum.Key]))
                {
                    return false;
                }
            }
            return true;
        }

        internal static bool CompareBinary(byte[] sent, byte[] recv)
        {
            if(sent.Length != recv.Length)
            {
                return false;
            }

            bool equal = true;
            for(int i=0; i<sent.Length && equal; i++)
            {
                equal = equal && sent[i] == recv[i];
            }
            return equal;
        }

        internal static bool Compare(object sent, object recv)
        {
            if(sent == null || recv == null)
            {
                return sent == null && recv == null;
            }

            Type sentType = sent.GetType();
            Type recvType = recv.GetType();

            if( sentType.Equals(recvType))
            {
                // compare value
                if(sentType.IsPrimitive)
                {
                    return recv.Equals(sent);
                }
                else if (sent is IList)
                {
                    return CompareList(sent as IList, recv as IList);
                }
                else if (sent is IDictionary)
                {
                    return CompareDictionary(sent as IDictionary, recv as IDictionary);
                }
                else if (sent is IPrimitiveMap)
                {
                    if (null == CompareMap(sent as IPrimitiveMap, recv as IPrimitiveMap))
                    {
                        return true;
                    }
                    else return false;
                }
                else if (sentType.IsArray && sent is byte[])
                {
                    return CompareBinary(sent as byte[], recv as byte[]);
                }
                else if (sent is String)
                {
                    return recv.Equals(sent);
                }
                else if (sent is ITopic)
                {
                    return (sent as ITopic).TopicName.Equals((recv as ITopic).TopicName);
                }
                else if (sent is IQueue)
                {
                    return (sent as IQueue).QueueName.Equals((recv as IQueue).QueueName);
                }
                else
                {
                    Assert.Fail("Unrecognizable object type {0} for comparison. Value : {1}", sentType.Name, sent);
                }
            }
            else if (recvType.IsEquivalentTo(sentType))
            {
                // list, Dictionary, and Map Implementations could be different
                if (sent is IList && recv is IList)
                {
                    return CompareList(sent as IList, recv as IList);
                }
                else if (sent is IDictionary && recv is IDictionary)
                {
                    return CompareDictionary(sent as IDictionary, recv as IDictionary);
                }
                else if (sent is IPrimitiveMap && recv is IPrimitiveMap)
                {
                    if (null == CompareMap(sent as IPrimitiveMap, recv as IPrimitiveMap)) return true;
                    else return false;
                }
                else
                {
                    Assert.Fail("Unreconcilable object types {0}, {1} for value {2}, {3}.", sentType.Name, recvType.Name, sent, recv);
                }
            }
            
            return false;
        }

        private static List<object> ReadStreamMessageBody(IStreamMessage message)
        {
            List<object> contents = new List<object>();
            try
            {
                while (true)
                {
                    object value = message.ReadObject();
                    contents.Add(value);
                }
            }
            catch (MessageEOFException eofEx)
            {
                Logger.Debug("Caught EOF exception while reading stream message. Ex = " + eofEx);
            }
            return contents;
        }

        internal static bool CompareStreamMessageBody(IStreamMessage sent, IStreamMessage recv)
        {
            if (sent == null || recv == null)
            {
                return sent == null && recv == null;
            }
            List<object> sentObjects = ReadStreamMessageBody(sent);
            List<object> recvObjects = ReadStreamMessageBody(recv);
            return CompareList(sentObjects, recvObjects);
        }

        /*
         * Determines message body type by interface.
         * type values:
         * MSG  = 0,
         * TEXT = 1,
         * STRM = 2,
         * OBJ  = 3,
         * MAP  = 4,
         * BYTE = 5
         * */
        private static int MsgBodyType(IMessage message)
        {
            int type = 0; // default to MSG
            if (message is ITextMessage)
            {
                type = 1;
            }
            else if (message is IStreamMessage)
            {
                type = 2;
            }
            else if (message is IObjectMessage)
            {
                type = 3;
            }
            else if (message is IMapMessage)
            {
                type = 4;
            }
            else if (message is IBytesMessage)
            {
                type = 5;
            }
            return type;
        }

        internal static bool CompareMsgBody(IMessage sent, IMessage recv)
        {
            if( sent == null || recv == null)
            {
                return sent == null && recv == null;
            }

            if (sent is IBytesMessage && recv is IBytesMessage)
            {
                return Compare((sent as IBytesMessage).Content, (recv as IBytesMessage).Content);
            }
            else if (sent is IMapMessage && recv is IMapMessage)
            {
                return Compare((sent as IMapMessage).Body, (recv as IMapMessage).Body);
            }
            else if (sent is IStreamMessage && recv is IStreamMessage)
            {
                return CompareStreamMessageBody((sent as IStreamMessage), (recv as IStreamMessage));
            }
            else if (sent is ITextMessage && recv is ITextMessage)
            {
                return Compare((sent as ITextMessage).Text, (recv as ITextMessage).Text);
            }
            else if (sent is IObjectMessage && recv is IObjectMessage)
            {
                return Compare((sent as IObjectMessage).Body, (recv as IObjectMessage).Body);
            }
            // Determine default case if msg types are different (return false) or types are IMessage with no body (return true).
            return MsgBodyType(sent) == MsgBodyType(recv);
        }

        internal static string CompareMsgProperties(IMessage sent, IMessage recv)
        {
            if (sent.NMSCorrelationID == null)
            {
                if (recv.NMSCorrelationID != null)
                {
                    return "Sent CorrelationID is null, Received is:" + recv.NMSCorrelationID;
                }
            }
            else if (sent.NMSCorrelationID != recv.NMSCorrelationID)
            {
                return "sent NMSCorrelationId is '"+sent.NMSCorrelationID
                    +"' != recv '"+recv.NMSCorrelationID+"'";
            }
            if (!Compare(sent.NMSDestination, recv.NMSDestination))
            {
                return "sent Destination != recv Destination";
            }
            // Time To Live will change in the network, but if set on sent it should
            // be less than or equal to the sent value when it is received
            if ( sent.NMSTimeToLive != null)
            {
                if (recv.NMSTimeToLive == null)
                {
                    return "Sent TimeToLive is null, Received is:" + recv.NMSTimeToLive;
                }
                if (recv.NMSTimeToLive > sent.NMSTimeToLive)
                {
                    return "Sent TTL="+sent.NMSTimeToLive+
                        " is less than Recv TTL="+recv.NMSTimeToLive;
                }
            }
            // NMSMessageId is set by the API, if set by the application, which it 
            // shouldn't be, it is quietly overwritten in the API. Still after send it
            // should be the same as what is recieved, just may no longer be what was
            // originally set.
            if (sent.NMSMessageId != recv.NMSMessageId)
            {
                return "Sent MessageID: '" + sent.NMSMessageId
                    + "' != Recv MessageID: '" + recv.NMSMessageId + "'";
            }
            if (sent.NMSDeliveryMode != recv.NMSDeliveryMode)
            {
                return "Sent DeliverMode != recv DeliveryMode";
            }
            //if (sent.NMSPriority != recv.NMSPriority)
            //{
            //    return "Sent Priority != Recv Priority";
            //}
            // NMSRedelivered is overwritten by the API if the application erroneously sets
            // it, so nothing to check here.
            if ( sent.NMSReplyTo != null)
            {
                if (recv.NMSReplyTo == null)
                {
                    return "Sent RepltTo non null and Recv ReplyTo is null";
                }
                if (!Compare(sent.NMSReplyTo,recv.NMSReplyTo))
                {
                    return "Sent ReplyTo != Recv ReplyTo";
                }
            }
            if (sent.NMSTimestamp != null)
            {
                if (recv.NMSTimestamp == null)
                {
                    return "Sent Timestamp is null, Received is:" + recv.NMSTimestamp;
                }
                // AMQPnetLite truncates the time at milliseconds so the DataTime sent will not
                // exactly match that received.  Just compare strings to the ms
                if (!sent.NMSTimestamp.ToString("yyyy-MM-ddTHH:mm:ss.fff").Equals(
                    recv.NMSTimestamp.ToString("yyyy-MM-ddTHH:mm:ss.fff")))
                {
                    return "Sent Timestamp= " + 
                        sent.NMSTimestamp.ToString("yyyy-MM-ddTHH:mm:ss.fff") +
                        " != " + recv.NMSTimestamp.ToString("yyyy-MM-ddTHH:mm:ss.fff");
                }
            }
            // NMSType is set by the API, if set by the application, which it 
            // shouldn't be, it is quietly overwritten in the API, so nothing to check
            // here.

            // Check the properties map
            return CompareMap(sent.Properties, recv.Properties);
        }

        internal static string CompareMsg(IMessage sent, IMessage recv)
        {
            string propertyCompare = CompareMsgProperties(sent, recv);
            string bodyCompare = CompareMsgBody(sent, recv) ? null : " Message Body does not match";
            if (null == propertyCompare)
            {
                return bodyCompare;
            } else if (null == bodyCompare)
            {
                return propertyCompare;
            } else
            {
                return propertyCompare + bodyCompare;
            }
        }

        #endregion

        protected IDestination Destination;
        protected IMessageProducer Producer;
        protected IMessageConsumer Consumer;
        protected ISession Session;
        protected IConnection Connection;
        public override void Setup()
        {
            base.Setup();
        }

        public override void TearDown()
        {
            base.TearDown();
            Connection = null;
            Session = null;
            Destination = null;
            Producer = null;
        }
        
        public IMessage SendReceiveMessage(IMessage sendingMsg, int timeout = 0)
        {
            if(Producer != null && sendingMsg != null)
            {
                try
                {
                    Producer.Send(sendingMsg);
                    if(Consumer != null && Connection.IsStarted)
                    {
                        return Consumer.Receive(timeout == 0 ? TIMEOUT : TimeSpan.FromMilliseconds(timeout));
                    }
                    else
                    {
                        return null;
                    }
                }
                catch (Exception ex)
                {
                    Logger.Error("Error sending message for Message " + this.GetTestMethodName() + ". Message : " + ex.Message + " stack: " + ex.StackTrace);
                    throw ex;
                }
                
            }
            else
            {
                return sendingMsg;
            }
        }

        [Test]
        [ProducerSetup("default", "test")]
        [TopicSetup("default", "test", Name = "nms.test")]
        [SessionSetup("dotnetConn", "default")]
        [ConnectionSetup("default", "dotnetConn", EncodingType = "dotnet")]
        [SkipTestOnRemoteBrokerProperties("dotnetConn", RemotePlatform = NMSTestConstants.NMS_SOLACE_PLATFORM)]
        public void TestObjectMessageDotnetEncoding()
        {
            using (Session = GetSession("default"))
            using (Producer = GetProducer())
            {
                object[] values = new object[]
                {
                    new Number(){ Value = 12354638},
                    new Number(){ Value = new Decimal(123456.1235468)},
                };
                try
                {
                    IMessage msg = null;
                    foreach (object value in values)
                    {
                        msg = Session.CreateObjectMessage(value);
                        if (Logger.IsDebugEnabled)
                            Logger.Debug(msg.ToString());
                        Producer.Send(msg);
                    }
                }
                catch (Exception e)
                {
                    PrintTestFailureAndAssert(GetMethodName(), "Unexpected Exception", e);
                }
            }
        }

        [Test]
        [ConnectionSetup("default", "default")]
        [SessionSetup("default", "default")]
        [TopicSetup("default", Name = "nms.test")]
        [ProducerSetup("default", DeliveryMode = MsgDeliveryMode.Persistent)]
        [SkipTestOnRemoteBrokerProperties("default", RemotePlatform = NMSTestConstants.NMS_SOLACE_PLATFORM)]
        public void TestObjectMessage()
        {
            using (Connection = GetConnection("default"))
            using (Session = GetSession("default"))
            using (Destination = GetDestination())
            using (Producer = GetProducer())
            {
                IMessage msg = null;
                try
                {
                    
                    IPrimitiveMap map = new PrimitiveMap();
                    map["myKey"] = "foo";
                    map["myOtherKey"] = 58;
                    map["myMapKey"] = new Dictionary<string, object>()
                    {
                        {
                            "test", new List<object>
                            {
                                null,
                                1.12f,
                                "barfoo"
                            }
                        },
                        { "key1", 0xfffff454564 }
                    };
                    object[] values = new object[]
                    {
                        null,
                        123456789,
                        1.12f,
                        55.84846,
                        3000000000L,
                        long.MaxValue,
                        int.MinValue,
                        map,
                        "FooBar",
                        new List<object>
                        {
                            null,
                            1.12f,
                            "barfoo"
                        },
                        new byte[]{ 0x66,0x65,0x77 },
                        'c',

                    };
                    foreach (object value in values)
                    {
                        msg = Session.CreateObjectMessage(value);
                        if (Logger.IsDebugEnabled)
                            Logger.Debug(msg.ToString());
                        Producer.Send(msg);
                    }

                }
                catch (Exception e)
                {
                    PrintTestFailureAndAssert(GetTestMethodName(), "Unexpected Exception", e);
                }

            }
        }

        [Serializable]
        protected class Number 
        {
            private Decimal value;

            internal Number() : this(Decimal.Zero) { }
            internal Number(Decimal val) { value = val; }
            
            public Decimal Value
            {
                get { return value; }
                set { this.value = value; }
            }
        }

        [Test]
        [ConnectionSetup(null, "default", MaxFrameSize = MaxFrameSize)]
        [SessionSetup("default", "s1")]
        [TopicSetup("s1", "t1", Name = "nms.test")]
        [ProducerSetup("s1", "t1", "sender", DeliveryMode = MsgDeliveryMode.Persistent)]
        [ConsumerSetup("s1", "t1", "receiver")]
        [SkipTestOnRemoteBrokerProperties("default", RemotePlatform = NMSTestConstants.NMS_SOLACE_PLATFORM)]
        public void TestStreamMessage()
        {
            /*
             * Test Values for IStreamMessage should include all types supported by the Stream Interface.
             * This includes : string, byte, short, int, long, float(Single), double, byte[], bool, char
             * 
             */
            object[] TestValues = new object[]
            {
                (byte)0xef,
                48,
                false,
                'c',
                "smallString",
                String256,
                1.12f,
                9223372036854775807L,
                55.123456789123456789,
                Bytes257,
                MaxBytes,
            };

            int TestSetSize = TestValues.Length;

            using (Connection = GetConnection("default"))
            using (Producer = GetProducer("sender"))
            using (Consumer = GetConsumer("receiver"))
            {
                try
                {
                    // setup connection callbacks
                    Connection.ExceptionListener += DefaultExceptionListener;

                    Connection.Start();

                    IStreamMessage sendMsg = Producer.CreateStreamMessage();
                    IStreamMessage recvMsg = null;
                    IMessage m = null;
                    object testValue = null;
                    for (int i=0; i<TestSetSize; i++)
                    {
                        testValue = TestValues[i];
                        
                        sendMsg.WriteObject(testValue);

                        m = SendReceiveMessage(sendMsg);

                        Assert.NotNull(m, "Failed to received message containing single value.");
                        Assert.IsTrue(m is IStreamMessage, "Received message is not the same type as sent message. Sent Message Type {0}", typeof(IStreamMessage).Name);
                        recvMsg = m as IStreamMessage;

                        object value = recvMsg.ReadObject();
                        
                        Assert.IsTrue(Compare(testValue, value),
                            "Stream message body object does not match sent message."
                          + " Send Object type {0}, value {1}"
                          + " Recv Object type {2}, value {3}",
                           testValue.GetType().Name, testValue.ToString(),
                           value?.GetType()?.Name, value?.ToString());

                        // add remaining test values.
                        for(int j = i+1; j < TestSetSize; j++)
                        {
                            sendMsg.WriteObject(TestValues[j]);
                        }

                        m = SendReceiveMessage(sendMsg);

                        Assert.NotNull(m, "Failed to received message containing multiple value.");
                        Assert.IsTrue(m is IStreamMessage, "Received message is not the same type as sent message. Sent Message Type {0}", typeof(IStreamMessage).Name);
                        recvMsg = m as IStreamMessage;

                        // turn send Msg to read mode
                        sendMsg.Reset();
                        
                        Assert.IsTrue(CompareStreamMessageBody(sendMsg, recvMsg), "Stream message body does not match sent message.");

                        // Clear mesage body for next iteration.
                        sendMsg.ClearBody();

                    }

                }
                catch(Exception ex)
                {
                    this.PrintTestFailureAndAssert(this.GetTestMethodName(), "Unexpected Exception", ex);
                }
            }

        }

        [Test]
        [ConnectionSetup(null, "default", MaxFrameSize = MaxFrameSize)]
        [SessionSetup("default", "s1")]
        [TopicSetup("s1", "t1", Name = "nms.test")]
        [ProducerSetup("s1", "t1", "sender", DeliveryMode = MsgDeliveryMode.Persistent)]
        [ConsumerSetup("s1", "t1", "receiver")]
        [SkipTestOnRemoteBrokerProperties("default", RemotePlatform = NMSTestConstants.NMS_SOLACE_PLATFORM)]
        public void TestMapMessage()
        {
            object[] MapValues = new object[]
            {
                48,
                "smallString",
                String256,
                //AlmostMaxString,
                1.12f,
                9223372036854775807L,
                55.123456789123456789,
                SmallDictionary,
                SmallList,
                /*LargeDictionary,
                LargeList,*/
                new List<object>()
                {
                    String256,
                    48,
                    SmallList,
                    SmallDictionary,
                    Bytes256
                },
                new Dictionary<string,object>()
                {
                    { "MyInt", 48 },
                    { "MyList", SmallList },
                    { "MyDictionary", SmallDictionary },
                    { "MyString", String256 }
                }
            };
            
            using (Connection = GetConnection("default"))
            using (Consumer = GetConsumer("receiver"))
            using (Producer = GetProducer("sender"))
            {
                try
                {
                    // setup connection callbacks
                    Connection.ExceptionListener += DefaultExceptionListener;
                    
                    Connection.Start();
                    

                    IMapMessage sendMsg = Producer.CreateMapMessage();
                    IMapMessage recvMsg = null;

                    // Add value to map message
                    int count = 0;
                    IPrimitiveMap map = sendMsg.Body;

                    foreach (object value in MapValues)
                    {
                        string key = GenerateMapKey(value, count);
                        count++;
                        map[key] = value;
                    }
                    
                    // Send and receive message to test Map body message encoding.
                    IMessage m = SendReceiveMessage(sendMsg, 180000);
                    Assert.NotNull(m, "Failed to Receive Message.");
                    Assert.IsTrue(m is IMapMessage, "Received message type does not match sent message type MAP. Type sent {0}, Type Receive {1}.", sendMsg.GetType(), m.GetType());

                    // Compare received message body to Test values.
                    recvMsg = m as IMapMessage;
                    Assert.AreEqual(map.Count, recvMsg.Body.Count, "Send map does not contain the same number of entries as receive map.");
                    Assert.IsTrue(CompareMsgBody(sendMsg, recvMsg), "Message bodies do not match.");
                    sendMsg.ClearBody();
                }
                catch (Exception ex)
                {
                    this.PrintTestFailureAndAssert(this.GetTestMethodName(), "Unexpected exception.", ex);
                }
            }
            

        }

        internal static string ToString(byte[] data)
        {
            if( data == null || data.Length == 0)
            {
                return (data == null) ? "null" : "[EMPTY]";
            }
            StringBuilder buffer = new StringBuilder(data.Length);
            buffer.Append("[");
            Array.ForEach<byte>(data, x => buffer.AppendFormat("{0},", x.ToString()));
            return buffer.Remove(buffer.Length - 1, 1).Append("]").ToString();
        }

        [Test]
        [ConnectionSetup(null, "default", MaxFrameSize = MaxFrameSize)]
        [SessionSetup("default", "s1")]
        [TopicSetup("s1", "t1", Name = "nms.test")]
        [ProducerSetup("s1", "t1", "sender", DeliveryMode = MsgDeliveryMode.Persistent)]
        [ConsumerSetup("s1", "t1", "receiver")]
        public void TestBytesMessage()
        {
            byte[][] TestValues = new byte[][]
            {
                new byte[]{ 0x00, 0x01, 0x02, 0x03, 0x04 },
                new byte[]{ 0x42, 0x79, 0x74, 0x65, 0x73 },
                Bytes256,
                Bytes257,
                MaxBytes,
            };
            using (Connection = GetConnection("default"))
            using (Consumer = GetConsumer("receiver"))
            using (Producer = GetProducer("sender"))
            {
                try
                {
                    Connection.ExceptionListener += DefaultExceptionListener;
                    Connection.Start();
                    IBytesMessage sendMsg = Producer.CreateBytesMessage();
                    IBytesMessage recvMsg = null;
                    IMessage m = null;
                    foreach(byte[] TestValue in TestValues)
                    {
                        //Logger.Warn("Writing TestValue: Length;" + TestValue.Length + " value;" + ToString(TestValue) );
                        sendMsg.WriteBytes(TestValue);

                        sendMsg.Reset();

                        Assert.AreEqual(TestValue.Length, sendMsg.BodyLength, "Message body length does not match Test Value length.");

                        Assert.IsTrue(Compare(TestValue, sendMsg.Content), "Failed to write Contents to message.");
                        
                        m = SendReceiveMessage(sendMsg);
                        Assert.NotNull(m, "Failed to receive Message. With Value : \"{0}\".", TestValue);
                        Assert.IsTrue(m is IBytesMessage, "Failed to receive message of the same type as sent message. Sent Message Type {0}, Receive Message Type {1}.", sendMsg.GetType().Name, m.GetType().Name);
                        recvMsg = m as IBytesMessage;
                        
                        Assert.IsTrue(Compare(sendMsg.Content, recvMsg.Content), "Message body does not match. Sent Value {0} Recv Value {1}", ToString(sendMsg.Content), ToString(recvMsg.Content));
                        sendMsg.ClearBody();
                        recvMsg.ClearBody();
                    }
                }
                catch (Exception ex)
                {
                    this.PrintTestFailureAndAssert(this.GetTestMethodName(), "Unexpected Exception", ex);
                }
            }
        }

        [Test]
        [ConnectionSetup(null, "default", MaxFrameSize = MaxFrameSize)]
        [SessionSetup("default", "s1")]
        [TopicSetup("s1", "t1", Name = "nms.test")]
        [ProducerSetup("s1", "t1", "sender", DeliveryMode = MsgDeliveryMode.Persistent)]
        [ConsumerSetup("s1", "t1", "receiver")]
        public void TestTextMessage()
        {
            string[] TestValues = new string[]
            {
                "text",
                "lllllllllllllongerString",
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-=_+,./<>?;':\"[]\\{}|",
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-=_+,./<>?;':\"[]\\{}|abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-=_+,./<>?;':\"[]\\{}|abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-=_+,.",
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-=_+,./<>?;':\"[]\\{}|abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-=_+,./<>?;':\"[]\\{}|abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-=_+,.?",
                String256,
                // Some brokers do not behave well with very large body values.
                /*
                AlmostMaxString,
                MaxString
                */
            };
            using (Connection = GetConnection("default"))
            using (Consumer = GetConsumer("receiver"))
            using (Producer = GetProducer("sender"))
            {
                try
                {
                    Connection.ExceptionListener += DefaultExceptionListener;
                    Connection.Start();

                    ITextMessage sendMsg = Producer.CreateTextMessage();
                    ITextMessage recvMsg = null;
                    IMessage m = null;

                    foreach (string value in TestValues)
                    {
                        sendMsg.Text = value;
                        m = SendReceiveMessage(sendMsg);
                        Assert.NotNull(m, "Failed to receive Message. With Value : \"{0}\".", (value.Length>512) ? (value.Substring(0,512) + "...") : value);
                        Assert.IsTrue(m is ITextMessage, "Failed to receive message of the same type as sent message. Sent Message Type {0}, Receive Message Type {1}.", sendMsg.GetType().Name, m.GetType().Name);
                        recvMsg = m as ITextMessage;
                        Assert.AreEqual(value, recvMsg.Text, "Message contents has changed from original value.");
                    }

                }
                catch (Exception ex)
                {
                    this.PrintTestFailureAndAssert(this.GetTestMethodName(), "Unexpected Exception", ex);
                }
            }
        }
        //
        // Create a Test Property Map that contains all the possible fields that 
        // can be set on the NMS interface, except those explicityly not supprted
        // according to AMQP 1.0 Section 3.2.5. (Array,  List, Dictionary).
        //
        internal static void setTestPropertyMap(IMessage message)
        {
            byte[] testBinArray = { 0xc0, 0xde, 0xbe, 0xad,
                                     0xad, 0xde, 0xef, 0xbe,
                                     0x23, 0x44, 0x55, 0x77};
            // Create a  test map, comments are the AMQP
            // expected type.
            message.Properties.SetBool("field_001", false); // BOOLEAN
            message.Properties.SetBool("field_002", true);  // BOOLEAN
            message.Properties.SetByte("field_003", 42);    // UINT8
            message.Properties.SetByte("field_004", 142);   // UINT8
            message.Properties.SetShort("field_008", -42);  // INT16
            message.Properties.SetShort("field_010", -142);  // INT16
            message.Properties.SetShort("field_012", -32768); // INT16
            message.Properties.SetInt("field_014", -80);     // INT32
            message.Properties.SetInt("field_016", -32768);  // INT32
            message.Properties.SetInt("field_018", -2147483648); // INT32
            message.Properties.SetLong("field_020", -80);        // INT64
            message.Properties.SetLong("field_022", -32768);     // INT64
            message.Properties.SetLong("field_024", -2147483648); // INT64
            message.Properties.SetLong("field_026", -9223372036854775808); // INT64
            message.Properties.SetChar("field_028", 'R'); // WCHAR
            message.Properties.SetBytes("field_029", testBinArray); // BINARY
            message.Properties.SetFloat("field_030", (float)3.14159); // FLOAT
            message.Properties.SetDouble("field_031", 3.14159); // DOUBLE
            message.Properties.SetString("field_032", "A short quick fox"); // STRING
            message.Properties.SetString("field_033", "43112609"); // STRING
            message.Properties.SetString("field_034", "3.14159"); // STRING
            message.Properties.SetString("field_035", "true"); // STRING
            message.Properties.SetString("field_036", "0"); // STRING
            message.Properties.SetString("field_037", "-43112609"); // STRING
        }
        //
        // Create a Test Property Map that contains field types not defined 
        // on the NMS interface, mostly unsigned values which exist in C# 
        // and in AMQP but not in Java so may be a problem for interop with
        // JMS and other applications implemented in limited languages.
        //
        // Also include a Byte Array, a List and a Dictionary, which can be set on
        // IPrimitiveMap but which are not supported according the the AMQP 1.0
        // specification.  ActiveMQ excepts and passes them fine.
        /// <param name="message"></param>
        //
        internal static void setTestPropertyMapExtended(IMessage message)
        {
            // Create a test map, comments are the AMQP
            // expected type.  As the NMS API does not provide setters
            // for these IPrimitive types, just use direct assignment.
            //
            message.Properties["field_005"] = (sbyte)42;    // INT8
            message.Properties["field_006"] = (sbyte)-42;   // INT8
            message.Properties["field_007"] = (ushort)42;  // UINT16
            message.Properties["field_009"] = (ushort)1042; // UINT16
            message.Properties["field_011"] = (ushort)65535; // UINT16
            message.Properties["field_013"] = (uint)255;    // UINT32
            message.Properties["field_015"] = (uint)65535;  // UINT32
            message.Properties["field_017"] = (uint)4294967295;   // UINT32
            message.Properties["field_019"] = (ulong)255;       // UINT64
            message.Properties["field_021"] = (ulong)65535;      // UINT64
            message.Properties["field_023"] = (ulong)4294967295; // UINT64
            message.Properties["field_025"] = (ulong)18446744073709551615; // UINT64
            message.Properties["field_038"] = null;             // NULL
            message.Properties.SetDictionary("field_039", SmallDictionary);
            message.Properties.SetList("field_040", SmallList);

        }
        [Test]
        [ConnectionSetup(null, "default", MaxFrameSize = MaxFrameSize)]
        [SessionSetup("default", "s1")]
        [TopicSetup("s1", "t1", Name = "nms.test")]
        [TopicSetup("s1", "tfail", Name ="nms.goingNoWhwere")]
        [TopicSetup("s1", "replyTo", Name = "nms.me")]
        [ProducerSetup("s1", "t1", "sender", DeliveryMode = MsgDeliveryMode.Persistent)]
        [ConsumerSetup("s1", "t1", "receiver")]
        [SkipTestOnRemoteBrokerProperties("default", RemotePlatform = NMSTestConstants.NMS_SOLACE_PLATFORM)]
        public void TestMessageProperties([Values(
            "unprefixed-Correlation-Id",
            "ID:jmsMessageId",
            "ID:AMQP_ULONG:52",
            "ID:AMQP_BINARY:ABCDEF",
            "ID_AMQP_STRING:ID:AMQP_ULONG:77232917",
            "AMQP_ULONG:foo",
            "AMQP_ULONG:77232917"
            )] string corrId)
        {
            using (Connection = GetConnection("default"))
            using (Consumer = GetConsumer("receiver"))
            using (Producer = GetProducer("sender"))
            {
                try
                {
                    Connection.ExceptionListener += DefaultExceptionListener;
                    Connection.Start();

                    ITextMessage sendMsg = Producer.CreateTextMessage();
                    ITextMessage recvMsg = null;
                    IMessage m = null;
                    string textForMsg = "Properties Message Test";

                    sendMsg.NMSCorrelationID = corrId;
                    sendMsg.NMSDestination = this.GetDestination("tfail");
                    sendMsg.NMSTimeToLive = new TimeSpan(0, 10, 0); // hours, mins, seconds
                    sendMsg.NMSMessageId = "message 1";
                    sendMsg.NMSDeliveryMode = MsgDeliveryMode.NonPersistent;
                    sendMsg.NMSPriority = MsgPriority.AboveNormal;
                    sendMsg.NMSRedelivered = true;
                    sendMsg.NMSReplyTo = this.GetDestination("replyTo");
                    sendMsg.NMSTimestamp = new DateTime(2017, 3, 18, 12, 40, 0);
                    sendMsg.NMSType = "MapThisMessage";
                    sendMsg.Text = textForMsg;
                    setTestPropertyMap ( sendMsg ) ;
                    setTestPropertyMapExtended(sendMsg);
                    m = SendReceiveMessage(sendMsg);
                    Assert.NotNull(m, 
                        "Failed to receive Message. With body text : \"{0}\".", 
                        textForMsg);
                    Assert.IsTrue(m is ITextMessage, 
                        "Failed to receive message of the same type as sent message. Sent Message Type {0}, Receive Message Type {1}.",
                        sendMsg.GetType().Name, m.GetType().Name);
                    recvMsg = m as ITextMessage;
                    Assert.AreEqual(textForMsg, recvMsg.Text,
                        "Message contents has changed from original value.");
                    Assert.Null(CompareMsg(sendMsg, recvMsg), 
                        "Message received does not match:" );

                }
                catch (Exception ex)
                {
                    this.PrintTestFailureAndAssert(this.GetTestMethodName(),
                        "Unexpected Exception", ex);
                }
            }
        }
        [Test]
        [ConnectionSetup(null, "default", MaxFrameSize = MaxFrameSize)]
        [SessionSetup("default", "s1")]
        [TopicSetup("s1", "t1", Name = "nms.test")]
        [TopicSetup("s1", "tfail", Name = "nms.goingNoWhwere")]
        [TopicSetup("s1", "replyTo", Name = "nms.me")]
        [ProducerSetup("s1", "t1", "sender", DeliveryMode = MsgDeliveryMode.Persistent)]
        [ConsumerSetup("s1", "t1", "receiver")]
        // Exactly the same test as TestMessageProperties but do not set 
        // any fields in the Properties map that are not specifcally allowed by
        // the NMS/JMS interface documentation.  So do not call 
        // setTestPropertyMapExtended()
        public void TestMessagePropertiesBasic([Values(
            "unprefixed-Correlation-Id",
            "ID:jmsMessageId",
            "ID:AMQP_ULONG:52",
            "ID:AMQP_BINARY:ABCDEF",
            "ID_AMQP_STRING:ID:AMQP_ULONG:77232917",
            "AMQP_ULONG:foo",
            "AMQP_ULONG:77232917"
            )] string corrId)
        {
            using (Connection = GetConnection("default"))
            using (Consumer = GetConsumer("receiver"))
            using (Producer = GetProducer("sender"))
            {
                try
                {
                    Connection.ExceptionListener += DefaultExceptionListener;
                    Connection.Start();

                    ITextMessage sendMsg = Producer.CreateTextMessage();
                    ITextMessage recvMsg = null;
                    IMessage m = null;
                    string textForMsg = "Properties Message Test";

                    sendMsg.NMSCorrelationID = corrId;
                    sendMsg.NMSDestination = this.GetDestination("tfail");
                    sendMsg.NMSTimeToLive = new TimeSpan(0, 10, 0); // hours, mins, seconds
                    sendMsg.NMSMessageId = "message 1";
                    sendMsg.NMSDeliveryMode = MsgDeliveryMode.NonPersistent;
                    sendMsg.NMSPriority = MsgPriority.AboveNormal;
                    sendMsg.NMSRedelivered = true;
                    sendMsg.NMSReplyTo = this.GetDestination("replyTo");
                    sendMsg.NMSTimestamp = new DateTime(2017, 3, 18, 12, 40, 0);
                    sendMsg.NMSType = "MapThisMessage";
                    sendMsg.Text = textForMsg;
                    setTestPropertyMap(sendMsg);
                    m = SendReceiveMessage(sendMsg);
                    Assert.NotNull(m,
                        "Failed to receive Message. With body text : \"{0}\".",
                        textForMsg);
                    Assert.IsTrue(m is ITextMessage,
                        "Failed to receive message of the same type as sent message. Sent Message Type {0}, Receive Message Type {1}.",
                        sendMsg.GetType().Name, m.GetType().Name);
                    recvMsg = m as ITextMessage;
                    Assert.AreEqual(textForMsg, recvMsg.Text,
                        "Message contents has changed from original value.");
                    Assert.Null(CompareMsg(sendMsg, recvMsg),
                        "Message received does not match:");

                }
                catch (Exception ex)
                {
                    this.PrintTestFailureAndAssert(this.GetTestMethodName(),
                        "Unexpected Exception", ex);
                }
            }
        }
        [Test]
        [ConnectionSetup(null, "default", MaxFrameSize = MaxFrameSize)]
        [SessionSetup("default", "s1")]
        [TopicSetup("s1", "t1", Name = "nms.test")]
        [TopicSetup("s1", "tfail", Name = "nms.goingNoWhwere")]
        [TopicSetup("s1", "replyTo", Name = "nms.me")]
        [ProducerSetup("s1", "t1", "sender", DeliveryMode = MsgDeliveryMode.Persistent)]
        [ConsumerSetup("s1", "t1", "receiver")]
        public void TestBadMessageProperties([Values(
            "ID:AMQP_ULONG:foo",
            "ID:AMQP_BINARY:ABCDEFGH"
            //, "ID_AMQP_STRING:77232917"  // TBD. JMS Mapping doc is not clear that this is illegal
            )] string corrId)
        {
            using (Connection = GetConnection("default"))
            using (Consumer = GetConsumer("receiver"))
            using (Producer = GetProducer("sender"))
            {
                try
                {
                    Connection.ExceptionListener += DefaultExceptionListener;
                    Connection.Start();

                    ITextMessage sendMsg = Producer.CreateTextMessage();
                    IMessage m = null;
                    string textForMsg = "Properties Message Test";

                    sendMsg.NMSCorrelationID = corrId;
                    sendMsg.NMSDestination = this.GetDestination("tfail");
                    sendMsg.NMSTimeToLive = new TimeSpan(0, 10, 0); // hours, mins, seconds
                    sendMsg.NMSMessageId = "message 1";
                    sendMsg.NMSDeliveryMode = MsgDeliveryMode.NonPersistent;
                    sendMsg.NMSPriority = MsgPriority.AboveNormal;
                    sendMsg.NMSRedelivered = true;
                    sendMsg.NMSReplyTo = this.GetDestination("replyTo");
                    sendMsg.NMSTimestamp = new DateTime(2017, 3, 18, 12, 40, 0);
                    sendMsg.NMSType = "MapThisMessage";
                    sendMsg.Text = textForMsg;
                    setTestPropertyMap(sendMsg);
                    setTestPropertyMapExtended(sendMsg);
                    m = SendReceiveMessage(sendMsg);
                    Assert.IsTrue(false, "SendReceiveMessage did not throw");

                }
                catch (Exception ex)
                {
                    if (!(ex is NMSException))
                    {
                        this.PrintTestFailureAndAssert(this.GetTestMethodName(),
                        "Unexpected Exception", ex);
                    }
                }
            }
        }
    }
}
