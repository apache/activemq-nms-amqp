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
using System.Collections;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using Apache.NMS;
using Apache.NMS.Util;
using Amqp.Framing;
using Amqp.Types;

namespace Apache.NMS.AMQP.Message.AMQP
{
    using Amqp;
    using Cloak;
    using Util;
    using Util.Types;
    
    class AMQPObjectMessageCloak : AMQPMessageCloak, IObjectMessageCloak
    {

        public static AMQPObjectEncodingType DEFAULT_ENCODING_TYPE = AMQPObjectEncodingType.AMQP_TYPE;

        private IAMQPObjectSerializer objectSerializer;

        #region Contructors
        internal AMQPObjectMessageCloak(Apache.NMS.AMQP.Connection c, AMQPObjectEncodingType type) : base(c)
        {
            InitializeObjectSerializer(type);
            Body = null;
        }

        internal AMQPObjectMessageCloak(MessageConsumer mc, Amqp.Message message) : base(mc, message)
        {
            if (message.Properties.ContentType.Equals(SymbolUtil.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE))
            {
                if(message.MessageAnnotations.Map.ContainsKey(MessageSupport.JMS_JAVA_ENCODING) 
                    && message.MessageAnnotations.Map[MessageSupport.JMS_JAVA_ENCODING].Equals(SymbolUtil.BOOLEAN_TRUE))
                {
                    InitializeObjectSerializer(AMQPObjectEncodingType.JAVA_SERIALIZABLE);
                }
                else
                {
                    InitializeObjectSerializer(AMQPObjectEncodingType.DOTNET_SERIALIZABLE);
                }
            }
            else
            {
                InitializeObjectSerializer(AMQPObjectEncodingType.AMQP_TYPE);
            }
        }

        #endregion

        #region Internal Properties Fields
        internal override byte JMSMessageType { get { return MessageSupport.JMS_TYPE_OBJ; } }
        #endregion

        #region Public IObjectMessageCloak Properties
        public AMQPObjectEncodingType Type { get { return this.objectSerializer.Type; } }

        public object Body
        {
            get
            {
                return this.objectSerializer.GetObject();
            }
            set
            {
                this.objectSerializer.SetObject(value);
            }
        }

        public override byte[] Content
        {
            get
            {
                return null;
            }
            set
            {
                
            }
        }

        #endregion

        #region IMessageCloak Copy Methods

        IObjectMessageCloak IObjectMessageCloak.Copy()
        {
            IObjectMessageCloak ocloak = new AMQPObjectMessageCloak(connection, this.objectSerializer.Type);
            this.CopyInto(ocloak);
            return ocloak;
        }
        

        protected override void CopyInto(IMessageCloak msg)
        {
            base.CopyInto(msg);
            if (msg is IObjectMessageCloak)
            {
                IObjectMessageCloak copy = msg as IObjectMessageCloak;
                if (copy is AMQPObjectMessageCloak)
                {
                    this.objectSerializer.CopyInto((copy as AMQPObjectMessageCloak).objectSerializer);
                }
                else
                {
                    this.objectSerializer.SetObject(copy.Body);
                }
            }
            
        }

        #endregion

        #region Private Methods

        private void InitializeObjectSerializer(AMQPObjectEncodingType type)
        {
            switch (type)
            {
                case AMQPObjectEncodingType.AMQP_TYPE:
                    objectSerializer = new AMQPTypeSerializer(this);
                    break;
                case AMQPObjectEncodingType.DOTNET_SERIALIZABLE:
                    objectSerializer = new DotnetObjectSerializer(this);
                    break;
                case AMQPObjectEncodingType.JAVA_SERIALIZABLE:
                    objectSerializer = new JavaObjectSerializer(this);
                    break;
                default:
                    throw NMSExceptionSupport.Create(new ArgumentException("Unsupported object encoding."));
            }
        }

        #endregion
    }

    #region IAMQPObjectSerializer

    #region IAMQPObjectSerializer Interface
    internal interface IAMQPObjectSerializer
    {
        Amqp.Message Message { get; }
        void SetObject(object o);
        object GetObject();

        void CopyInto(IAMQPObjectSerializer serializer);

        AMQPObjectEncodingType Type { get; }

    }

    #endregion

    #region AMQP Type IAMQPObjectSerializer Implementation
    class AMQPTypeSerializer : IAMQPObjectSerializer
    {
        
        private readonly Amqp.Message amqpMessage;
        private readonly AMQPObjectMessageCloak message;
        internal AMQPTypeSerializer(AMQPObjectMessageCloak msg)
        {
            amqpMessage = msg.AMQPMessage;
            message = msg;
            msg.SetMessageAnnotation(MessageSupport.JMS_AMQP_TYPE_ENCODING, SymbolUtil.BOOLEAN_TRUE);
        }

        public Message Message { get { return amqpMessage; } }

        public AMQPObjectEncodingType Type { get { return AMQPObjectEncodingType.AMQP_TYPE; } }

        public void CopyInto(IAMQPObjectSerializer serializer)
        {
            serializer.SetObject(GetObject());
        }

        public object GetObject()
        {
            RestrictedDescribed body = amqpMessage.BodySection;
            if(body == null)
            {
                return null;
            }
            else if (body is AmqpValue)
            {
                AmqpValue value = body as AmqpValue;
                return value.Value;
            }
            else if (body is Data)
            {
                return (body as Data).Binary;
            }
            else if (body is AmqpSequence)
            {
                return (body as AmqpSequence).List;
            }
            else
            {
                throw new IllegalStateException("Unexpected body type: " + body.GetType().Name);
            }
        }

        public void SetObject(object o)
        {
            if(o == null)
            {
                amqpMessage.BodySection = MessageSupport.NULL_AMQP_VALUE_BODY;
            }
            else if (IsNMSObjectTypeSupported(o))
            {
                object value = null;
                if(o is IList)
                {
                    value = ConversionSupport.ListToAmqp(o as IList);
                }
                else if (o is IPrimitiveMap)
                {
                    value = ConversionSupport.NMSMapToAmqp(o as IPrimitiveMap);
                }
                else
                {
                    value = o;
                }
                // to copy the object being set encode a message then decode and take body
                Amqp.Message copy = new Amqp.Message(value);
                ByteBuffer buffer = copy.Encode();
                copy = Message.Decode(buffer);
                
                amqpMessage.BodySection = new AmqpValue { Value = copy.Body };
            }
            else
            {
                throw new ArgumentException("Encoding unexpected object type: " + o.GetType().Name);
            }
        }

        private bool IsNMSObjectTypeSupported(object o)
        {
            return ConversionSupport.IsNMSType(o) || o is List || o is Map || o is IPrimitiveMap || o is IList;
        }
    }

    #endregion

    #region Dotnet Serializable IAMQPObjectSerializer Implementation

    class DotnetObjectSerializer : IAMQPObjectSerializer
    {
        private readonly Amqp.Message amqpMessage;
        private readonly AMQPObjectMessageCloak message;
        internal DotnetObjectSerializer(AMQPObjectMessageCloak msg)
        {
            amqpMessage = msg.AMQPMessage;
            message = msg;
            msg.SetContentType(SymbolUtil.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);
            msg.SetMessageAnnotation(MessageSupport.JMS_DONET_ENCODING, SymbolUtil.BOOLEAN_TRUE);
        }

        public Message Message { get { return amqpMessage; } }

        public AMQPObjectEncodingType Type { get { return AMQPObjectEncodingType.DOTNET_SERIALIZABLE; } }

        public void CopyInto(IAMQPObjectSerializer serializer)
        {
            serializer.SetObject(GetObject());
        }

        public object GetObject()
        {
            byte[] bin = null;
            if(Message.BodySection == null)
            {
                return null;
            }
            else if (Message.BodySection is Data)
            {
                Data data = Message.BodySection as Data;
                bin = data.Binary;
            }
            // TODO handle other body types.

            if (bin == null || bin.Length == 0)
            {
                return null;
            }
            else
            {
                return GetDeserializedObject(bin);
            }
            
        }

        public void SetObject(object o)
        {
            
            byte[] bin = GetSerializedObject(o);
            if(bin == null || bin.Length == 0)
            {
                amqpMessage.BodySection = MessageSupport.EMPTY_DATA;
            }
            else
            {
                amqpMessage.BodySection = new Data() { Binary = bin };
            }
            
        }

        private object GetDeserializedObject(byte[] binary)
        {
            object result = null;

            MemoryStream stream = null;
            IFormatter formatter = null;
            try
            {
                stream = new MemoryStream(binary);
                formatter = new BinaryFormatter();
                result = formatter.Deserialize(stream);
            }
            finally
            {
                stream?.Close();
            }


            return result;

        }

        private byte[] GetSerializedObject(object o)
        {
            if (o == null) return new byte[] { 0xac,0xed,0x00,0x05,0x70 };
            MemoryStream stream = null;
            IFormatter formatter = null;
            byte[] result = null;
            try
            {
                stream = new MemoryStream();
                formatter = new BinaryFormatter();
                formatter.Serialize(stream, o);
                result = stream.ToArray();
            }
            finally
            {
                if(stream!= null)
                {
                    stream.Close();
                }
            }
            
            return result;
        }
    }

    #endregion

    #region Java Serializable IAMQPObjectSerializer Implementation

    class JavaObjectSerializer : IAMQPObjectSerializer
    {
        private readonly Amqp.Message amqpMessage;
        private readonly AMQPObjectMessageCloak message;
        internal JavaObjectSerializer(AMQPObjectMessageCloak msg)
        {
            amqpMessage = msg.AMQPMessage;
            message = msg;
            message.SetContentType(SymbolUtil.SERIALIZED_JAVA_OBJECT_CONTENT_TYPE);
            message.SetMessageAnnotation(MessageSupport.JMS_JAVA_ENCODING, SymbolUtil.BOOLEAN_TRUE);
        }

        public Message Message { get { return amqpMessage; } }

        public AMQPObjectEncodingType Type { get { return AMQPObjectEncodingType.JAVA_SERIALIZABLE; } }

        public void CopyInto(IAMQPObjectSerializer serializer)
        {
            // TODO fix to copy java serialized object as binary.
            serializer.SetObject(GetObject());
        }

        public object GetObject()
        {
            throw new NotImplementedException("Java Serialized Object body Not Supported.");
        }

        public void SetObject(object o)
        {
            throw new NotImplementedException("Java Serialized Object body Not Supported.");
        }
    }

    #endregion // Java IAMQPObjectSerializer Impl

    #endregion // IAMQPObjectSerializer
}
