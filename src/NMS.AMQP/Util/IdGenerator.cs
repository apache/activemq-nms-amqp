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
using System.Net;
using System.Security.Permissions;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.NMS.Util;
using Apache.NMS;

namespace Apache.NMS.AMQP.Util
{
    #region Id Class

    public class Id : IComparable
    {
        public static readonly Id EMPTY = new Id();
        protected const int DEFAULT_MAX_CAPACITY = 1;
        protected static readonly int[] HashTable = new int[] { 2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97};
        protected static readonly int HashTableSize = HashTable.Length;
        protected delegate ComponentId InstanceFactory(object o);
        protected static readonly Dictionary<Type, InstanceFactory> buildMap;

        #region Class Initializer

        static Id()
        {
            buildMap = new Dictionary<Type, InstanceFactory>();
            buildMap.Add(typeof(String), (o) => { return new ComponentId<string>(o as string); });
            buildMap.Add(typeof(UInt16), (o) => { return new ComponentId<UInt16>(Convert.ToUInt16(o)); });
            buildMap.Add(typeof(UInt32), (o) => { return new ComponentId<UInt32>(Convert.ToUInt32(o)); });
            buildMap.Add(typeof(UInt64), (o) => { return new ComponentId<UInt64>(Convert.ToUInt64(o)); });
            buildMap.Add(typeof(Int16), (o) => { return new ComponentId<UInt16>(Convert.ToUInt16(o)); });
            buildMap.Add(typeof(Int32), (o) => { return new ComponentId<UInt32>(Convert.ToUInt32(o)); });
            buildMap.Add(typeof(Int64), (o) => { return new ComponentId<UInt64>(Convert.ToUInt64(o)); });
            buildMap.Add(typeof(AtomicSequence), (o) =>
            {
                ulong val = 0;
                AtomicSequence seq = o as AtomicSequence;
                if (o != null && seq != null)
                {
                    val = seq.getAndIncrement();
                }
                return new ComponentId<UInt64>(val);
            });
            buildMap.Add(typeof(Guid), (o) =>
            {
                Guid id;
                if (o == null)
                {
                    id = Guid.Empty;
                }
                else
                {
                    id = (Guid)o;
                }
                return new ComponentId<Guid>(id);
            });
            buildMap.Add(typeof(Id), (o) => { return new ComponentId<Id>(o as Id); });
        }

        #endregion

        ComponentId[] components;
        private bool isReadOnly = false;
        private int length;
        private int maxCapacity;
        private int current;
        private string componentDelimeter;
        private int hashcode = 0;

        #region Constructors

        public Id(string delimeter, int size, int maxSize)
        {
            maxCapacity = Math.Max(maxSize, DEFAULT_MAX_CAPACITY);
            length = size;
            components = new ComponentId[length];
            current = 0;
            componentDelimeter = delimeter;
        }

        public Id(int size, int maxSize) : this(IdGenerator.ID_COMPONENT_DELIMETER, size, maxSize) { }

        public Id(int size) : this(IdGenerator.ID_COMPONENT_DELIMETER, size, size) { }

        public Id(params object[] args) : this(args.Length)
        {
            int added = this.AddAll(args);
            if(added > maxCapacity)
            {
                Tracer.ErrorFormat("Id Format error.");
            }
            Generate();
        }

        #endregion

        #region Public Properties

        public int Size
        {
            get { return length; }
        }

        #endregion

        #region Public Methods

        public void Add(object component)
        {
            Tracer.DebugFormat("Adding Component To Id, component {0}, current index: {1}", component, current);
            if (isReadOnly)
            {
                throw new NMSException("Invalid Operation when generating Component Id. Can not change id once generated.");
            }
            if (current >= maxCapacity)
            {
                throw new NMSException("Invalid Operation when generating Component Id. Can not add component. Adding Compoenent at full capacity " + maxCapacity);
            }
            if (current >= length)
            {
                grow(length * 2);
            }
            Type type = component.GetType();
            InstanceFactory instf = null;
            buildMap.TryGetValue(type, out instf);
            if (instf == null)
            {
                throw new NMSException(string.Format("Invalid Id component type {0} for component {1}", type.ToString(), component.ToString()));
            }
            components[current] = instf(component);
            current++;

        }

        public object[] GetComponents(int startIndex=0)
        {
            return GetComponents(startIndex, length);
        }

        public object[] GetComponents(int startIndex, int endIndex)
        {
            int eIndex = Math.Max(0,Math.Min(endIndex, length));
            int sIndex = Math.Max(0, startIndex);
            int resultLen = eIndex - sIndex;
            if (resultLen<0)
            {
                return null;
            }
            object[] comps = new object[resultLen];
            int index = 0;
            for(int i=sIndex; i< eIndex; i++)
            {
                comps[index] = components[i].IdValue;
                index++;
            }
            return comps;
        }

        public object GetComponent(int index)
        {
            if (isReadOnly)
            {
                if (index >= 0 && index < length)
                {
                    return components[index].IdValue;
                }
            }
            return null;
        }

        public object GetFirstComponent(Type type)
        {
            if (isReadOnly)
            {
                for (int i = 0; i < length; i++)
                {
                    ComponentId cid = components[i];
                    if (cid.ValueType.Equals(type))
                    {
                        return cid.IdValue;
                    }
                }
            }
            return null;
        }

        public object GetLastComponent(Type type)
        {
            if (isReadOnly)
            {
                for (int i = length; i > 0; i--)
                {
                    ComponentId cid = components[i - 1];
                    if (cid.ValueType.Equals(type))
                    {
                        return cid.IdValue;
                    }
                }
            }
            return null;
        }

        public void Generate()
        {
            if (!isReadOnly)
            {
                isReadOnly = true;
                this.GetHashCode();
            }
        }

        #endregion

        #region Object Override Methods

        public override bool Equals(object obj)
        {
            if(GetHashCode() == obj.GetHashCode())
            {
                return true;
            }
            else
            {
                return CompareTo(obj) == 0;
            }
        }

        public override int GetHashCode()
        {
            if(hashcode == 0 && isReadOnly && length > 0)
            {
                int hashIndex = 0;
                ComponentId cid = components[0];
                hashcode = HashTable[hashIndex] * cid.GetHashCode();
                for (int i=1; i<length; i++)
                {
                    cid = components[i];
                    hashIndex = i % HashTableSize;
                    hashcode = hashcode ^ HashTable[hashIndex] * cid.GetHashCode(); 
                }
            }
            return hashcode;
        }

        public override string ToString()
        {
            if (maxCapacity == 0)
            {
                return "0";
            }
            if (isReadOnly)
            {
                if (length == 0)
                {
                    return EMPTY.ToString();
                }
                StringBuilder sb = new StringBuilder();
                ComponentId cid = this.components[0];
                sb.Append(cid.ToString());
                for (int i = 1; i < length; i++)
                {
                    cid = this.components[i];
                    sb.Append(componentDelimeter);
                    sb.Append(cid.ToString());
                    
                }
                return sb.ToString();
            }
            else
            {
                return base.ToString();
            }
        }

        #endregion

        #region IComparable Methods

        public int CompareTo(object obj)
        {

            if(obj!=null && obj is Id)
            {
                return CompareTo(obj as Id);
            }
            else if(obj == null)
            {
                return 1;
            }
            else
            {
                return -1;
            }

        }

        public int CompareTo(Id that)
        {
            if(this.length > that.length)
            {
                return 1;
            }
            else if (this.length < that.length)
            {
                return -1;
            }
            else
            {
                int compare = 0;
                for(int i=0; i<length; i++)
                {
                    ComponentId thisCid = this.components[i];
                    ComponentId thatCid = that.components[i];
                    compare = thisCid.CompareTo(thatCid);
                    if ( compare > 0)
                    {
                        return 1;
                    }
                    else if ( compare < 0 )
                    {
                        return -1;
                    }

                }
                return compare;
            }
        }

        #endregion

        #region Protected Methods

        protected void grow(int newCapacity)
        {
            int size = Math.Min(newCapacity, maxCapacity);
            ComponentId[] buffer = new ComponentId[size];
            Array.Copy(this.components, buffer, this.length);
            length = size;
            this.components = buffer;
        }

        protected int AddAll(params object[] args)
        {
#if NET46
            if(Tracer.IsDebugEnabled)
            {
                Tracer.DebugFormat("Adding Id components: {0} MaxCapacity: {1}", string.Join(",",
                          args.Select(x => x.ToString()).ToArray()), maxCapacity);

            }
#else
#endif
            int added = 0;
            foreach (object o in args)
            {
                Type type = o.GetType();

                if (type.IsArray && type.Equals(typeof(object[])))
                {
                    object[] moreArgs = o as object[];
                    int addlen = (moreArgs).Length;

                    maxCapacity = maxCapacity + addlen - 1;
                    added += this.AddAll(moreArgs);

                }
                else
                {
                    this.Add(o);
                    added++;
                }
            }
            return added;
        }

#endregion

#region Inner ComponentId Classes

        protected abstract class ComponentId : IComparable
        {
            protected object value;

            protected ComponentId(object idvalue)
            {
                value = idvalue;
            }

            public object IdValue { get { return value; } }

            public abstract Type ValueType { get; }

            public virtual int CompareTo(object obj)
            {
                if(obj == null)
                {
                    return 1;
                }
                else if(obj is ComponentId)
                {
                    return CompareTo(obj as ComponentId);
                }
                else if (obj is IComparable)
                {
                    return -1 * (obj as IComparable).CompareTo(this.IdValue);
                }
                else
                {
                    return -1;
                }
            }

            public virtual int CompareTo(ComponentId that)
            {
                if (this.ValueType.Equals(that.ValueType) || this.ValueType.IsEquivalentTo(that.ValueType))
                {
                    if (this.IdValue.Equals(that.IdValue))
                    {
                        return 0;
                    }
                    else
                    {
                        return this.GetHashCode() - that.GetHashCode();
                    }
                }
                else if (this.IdValue is IComparable)
                {
                    return (this.IdValue as IComparable).CompareTo(that.IdValue);
                }
                else if (that.IdValue is IComparable)
                {
                    return -1 * (that.IdValue as IComparable).CompareTo(this.IdValue);
                }
                else
                {
                    return this.ValueType.GetHashCode() - that.ValueType.GetHashCode();
                }
                
            }

            public override bool Equals(object obj)
            {
                return CompareTo(obj) == 0;
            }


            public override int GetHashCode()
            {
                return this.IdValue.GetHashCode();
            }

            public override string ToString()
            {
                return value.ToString();
            }
        }
        
        protected class ComponentId<T> : ComponentId
        {
            public ComponentId(T val) : base(val)
            {
            }

            public T Value
            {
                get { return (T)this.value; }
            }

            public override Type ValueType
            {
                get
                {
                    if (value != null)
                    {
                        return value.GetType();
                    }
                    return default(T).GetType();
                }
            }
        }

#endregion

    }

#endregion

#region IdGenerator Class

    public class IdGenerator
    {
        public const string ID_COMPONENT_DELIMETER = ":";
        protected static readonly string DEFAULT_PREFIX = "ID";
        protected static string hostname = null;

        protected readonly string prefix;
        protected readonly AtomicSequence sequence = new AtomicSequence(1);

#region Class Initializer

        static IdGenerator()
        {
#if NETSTANDARD2_0
            hostname = Dns.GetHostName();
#else
            DnsPermission permissions = null;
            try
            {
                permissions = new DnsPermission(PermissionState.Unrestricted);
            }
            catch (Exception e)
            {
                Tracer.InfoFormat("{0}", e.StackTrace);
            }
            if (permissions != null)
            {
                hostname = Dns.GetHostName();
            }
#endif

        }

#endregion

#region Constructors

        public IdGenerator(string prefix)
        {
            this.prefix = RemoveEnd(prefix, ID_COMPONENT_DELIMETER)
                + ((hostname == null)
                    ?
                    ""
                    :
                    ID_COMPONENT_DELIMETER + hostname)
                ;
        }

        public IdGenerator() : this(DEFAULT_PREFIX)
        {
        }

#endregion

        public virtual Id GenerateId()
        {
            Id id = new Id(this.prefix, Guid.NewGuid(), sequence);
            return id;
        }

        public virtual string generateID()
        {
            Id id = GenerateId();
            return id.ToString();
            //return string.Format("{0}{1}" + ID_COMPONENT_DELIMETER + "{2}", this.prefix, Guid.NewGuid().ToString(), sequence.getAndIncrement());
        }

        protected static string RemoveEnd(string s, string end)
        {
            string result = s;

            if (s != null && end != null && s.EndsWith(end))
            {
                int sLen = s.Length;
                int endLen = end.Length;
                int newLen = sLen - endLen;
                if (endLen > 0 && newLen > 0)
                {
                    StringBuilder sb = new StringBuilder(s, 0, newLen, newLen);
                    result = sb.ToString();
                }
            }
            return result;
        }
    }

#endregion

#region Derivative IdGenerator Classes

    class NestedIdGenerator : IdGenerator
    {
        protected Id parentId;
        protected bool removeParentPrefix;

        public NestedIdGenerator(string prefix, Id pId, bool remove) : base(prefix)
        {
            parentId = pId;
            removeParentPrefix = remove;
        }

        public NestedIdGenerator(string prefix, Id pId) : this(prefix, pId, false) { }

        public NestedIdGenerator(Id pId):this(DEFAULT_PREFIX, pId) { }

        public override Id GenerateId()
        {
            Id id;
            if (removeParentPrefix)
            {
                int componentIndex = (parentId.Size == 1) ? 0 : 1;
                id = new Id(prefix, parentId.GetComponents(componentIndex), sequence);
            }
            else
            {
                id = new Id(prefix, parentId, sequence);
            }
            
            return id;
        }

        public override string generateID()
        {
            return GenerateId().ToString();
        }
    }

    class CustomIdGenerator : IdGenerator
    {
        protected object[] parts;
        protected bool isOnlyParts;


        public CustomIdGenerator(string prefix, params object[] args) : base(prefix)
        {
            parts = args;
        }

        public CustomIdGenerator(bool onlyParts, params object[] args) : this(DEFAULT_PREFIX, args)
        {
            isOnlyParts = onlyParts;
        }

        public CustomIdGenerator(Id pId) : this(DEFAULT_PREFIX, pId) { }

        public override Id GenerateId()
        {
            Id id;
            if (isOnlyParts)
            {
                id = new Id(parts);
            }
            else
            {
                id = new Id(prefix, parts, sequence);
            }
            return id;
        }

        public override string generateID()
        {
            return GenerateId().ToString();
        }
    }

#endregion
}
