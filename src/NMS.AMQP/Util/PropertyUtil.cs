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
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Reflection;
using System.Text;
using Apache.NMS.AMQP.Transport;

namespace Apache.NMS.AMQP.Util
{
    public class PropertyUtil
    {
        private const string PROPERTY_TERM_SEPARATOR = ".";
        public const string PROPERTY_PREFIX = "NMS" + PROPERTY_TERM_SEPARATOR;
        
        public static string CreateProperty(string name, string subprefix = "", string prefix = PROPERTY_PREFIX)
        {
            string propertyPrefix = prefix +
                (prefix.Length > 0 && !prefix.EndsWith(PROPERTY_TERM_SEPARATOR)
                    ?
                    PROPERTY_TERM_SEPARATOR
                    :
                    ""
                 );

            string subPropertyTerm = subprefix +
                (subprefix.Length > 0 && !subprefix.EndsWith(PROPERTY_TERM_SEPARATOR)
                    ?
                    PROPERTY_TERM_SEPARATOR
                    :
                    ""
                );
            return propertyPrefix + subPropertyTerm + name;
        }

        public static void SetProperties(object obj, StringDictionary properties, string propertyPrefix = PROPERTY_PREFIX)
        {
            Dictionary<string, PropertyInfo> props = GetPropertiesForClass(obj);
            foreach (string rawkey in properties.Keys)
            {
                string key = RemovePrefix(propertyPrefix, rawkey);
                Tracer.DebugFormat("Searching for Property: \"{0}\"", key);
                if (props.ContainsKey(key))
                {
                    Tracer.DebugFormat(
                        "Assigning Property {0} to {1}.{2} with value {3}",
                        key, obj.GetType().Namespace, obj.GetType().Name, properties[rawkey]
                        );
#if NET40
                    if (props[key].GetSetMethod() != null)
#else
                    if(props[key].SetMethod!=null)
#endif
                        props[key].SetValue(obj, ConvertType(props[key].PropertyType, properties[rawkey]), null);
                }

            }
        }

        public static StringDictionary GetProperties(object obj, string propertyPrefix = PROPERTY_PREFIX)
        {
            StringDictionary result = new StringDictionary();
            Dictionary<string, PropertyInfo> props = GetPropertiesForClass(obj);
            string propsPrefix = propertyPrefix +
                (
                propertyPrefix.Length > 0 && !propertyPrefix.EndsWith(PROPERTY_TERM_SEPARATOR)
                ?
                    PROPERTY_TERM_SEPARATOR
                :
                    ""
                );
            foreach (string key in props.Keys)
            {
                object value = props[key].GetValue(obj, null);
                if (value != null)
                {
                    result[propertyPrefix + key] = value.ToString();
                }
            }
            return result;
        }

        public static object ConvertType(Type targetType, string value)
        {
            if (targetType.IsPrimitive)
            {
                return Convert.ChangeType(value, targetType);
            }
            else if (targetType.Equals(typeof(string)))
            {
                return value;
            }
            return null;
        }

        private static string RemovePrefix(string prefix, string propertyName)
        {
            if (propertyName.StartsWith(prefix, StringComparison.CurrentCultureIgnoreCase) && prefix.Length > 0)
            {
                return propertyName.Remove(0, prefix.Length);
            }
            else
            {
                return propertyName;
            }
        }

        public static Dictionary<string, PropertyInfo> GetPropertiesForClass(object obj)
        {
            MemberInfo[] members = obj.GetType().GetMembers();
            Dictionary<string, PropertyInfo> properties = new Dictionary<string, PropertyInfo>();
            foreach (MemberInfo member in members)
            {
                string memberName = member.Name;
                if (member != null && member is PropertyInfo)
                {
                    PropertyInfo prop = member as PropertyInfo;
                    properties.Add(memberName.ToLower(), prop);
                }
            }
            return properties;
        }
        
        public static StringDictionary Clone(StringDictionary original)
        {
            StringDictionary clone = new StringDictionary();
            foreach (string key in original.Keys)
            {
                clone.Add(key?.Clone() as string, original[key]?.Clone() as string);
            }
            return clone;
        }

        /// <summary>
        /// See <see cref="Merge(StringDictionary, StringDictionary, out StringDictionary, string, string, string)"/>
        /// </summary>
        /// <param name="one"></param>
        /// <param name="other"></param>
        /// <param name="onePrefix"></param>
        /// <param name="otherPrefix"></param>
        /// <param name="mergePrefix"></param>
        /// <returns></returns>
        public static StringDictionary Merge(
            StringDictionary one, 
            StringDictionary other, 
            string onePrefix = PROPERTY_PREFIX, 
            string otherPrefix = PROPERTY_PREFIX,
            string mergePrefix = PROPERTY_PREFIX
            )
        {
            StringDictionary d;
            return Merge(one, other, out d, onePrefix, otherPrefix, mergePrefix);
        }


        /// <summary>
        /// Takes all properties from one StringDictionary and merges them with the other StringDictionary.
        /// The properties in "one" are prefered over the "other".
        /// 
        /// </summary>
        /// <param name="one">StringDictionary containing properties.</param>
        /// <param name="other">Another StringDictionary containing properties.</param>
        /// <param name="cross">Holds all the properties from the "other" StringDictionary that are not used because one has the properties.</param>
        /// <param name="onePrefix">Optional string prefix for the properties in "one".</param>
        /// <param name="otherPrefix">Optional string prefix for the properties in "other".</param>
        /// <param name="mergePrefix">Optional string prefix for the properties in result.</param>
        /// <returns>Merged StringDictionary with properties from both "one" and "other".</returns>
        public static StringDictionary Merge(
            StringDictionary one, 
            StringDictionary other, 
            out StringDictionary cross, 
            string onePrefix = PROPERTY_PREFIX, 
            string otherPrefix = PROPERTY_PREFIX,
            string mergePrefix = PROPERTY_PREFIX
            )
        {
            if (one == null && other != null)
            {
                cross = null;
                return Clone(other);
            }
            else if (one!=null && other == null)
            {
                cross = null;
                return Clone(one);
            }
            else if (one == null && other == null)
            {
                cross = null;
                return new StringDictionary();
            }
            StringDictionary result = new StringDictionary();
            StringDictionary dups = new StringDictionary();
            
            Array arr = new string[other.Keys.Count];
            other.Keys.CopyTo(arr, 0);
            ArrayList otherKeys = new ArrayList(arr); 

            string mPre = mergePrefix +
                (
                mergePrefix.Length > 0 && !mergePrefix.EndsWith(PROPERTY_TERM_SEPARATOR)
                ?
                    PROPERTY_TERM_SEPARATOR
                :
                    ""
                );
            mergePrefix.ToLower();

            string onePre = onePrefix +
                (
                onePrefix.Length > 0 && !onePrefix.EndsWith(PROPERTY_TERM_SEPARATOR)
                ?
                    PROPERTY_TERM_SEPARATOR
                :
                    ""
                );
            onePre.ToLower();

            string otherPre = otherPrefix +
                (
                otherPrefix.Length > 0 && !otherPrefix.EndsWith(PROPERTY_TERM_SEPARATOR)
                ?
                    PROPERTY_TERM_SEPARATOR
                :
                    ""
                );
            otherPre.ToLower();

            foreach (string rawkey in one.Keys)
            {
                string key = RemovePrefix(onePre, rawkey);
                string otherkey = (otherPre + key).ToLower();
                string mergekey = (mPre + key).ToLower();
                if (!result.ContainsKey(mergekey))
                {
                    result.Add(mergekey, one[rawkey]);
                }
                if (other.ContainsKey(otherkey))
                {
                    otherKeys.Remove(otherkey);
                    dups.Add(mergekey, other[otherkey]);
                }
            }

            foreach (string rawkey in otherKeys)
            {
                string key = RemovePrefix(otherPre, rawkey);
                result.Add(mPre + key, other[rawkey]);
            }

            cross = dups.Count == 0 ? null : dups;
            return result;
        }

        public static string ToString(IList set)
        {
            if (set == null)
            {
                return "null";
            }
            if (set.Count == 0)
            {
                return "[]";
            }
            string result = "[";
            foreach (object o in set)
            {
                result += o.ToString() + ",";
            }
            return result.Substring(0, result.Length - 1) + "]";
        }

        private static string ToString(ArrayList set)
        {
            if(set == null)
            {
                return "null";
            }
            if(set.Count == 0)
            {
                return "[]";
            }
            string result = "[";
            foreach(object o in set)
            {
                result += o.ToString() + ",";
            }
            return result.Substring(0,result.Length - 1) + "]";
        }

        public static string ToString(StringDictionary properties)
        {
            if(properties == null)
            {
                return "null";
            }
            if (properties.Count == 0)
            {
                return "[]";
            }
            string result = "[\n";
            foreach(string s in properties.Keys)
            {
                result += string.Format("{0} : {1},\n", s, properties[s]);
            }
            return result.Substring(0,result.Length-2) + "\n]";
        }

        public static IDictionary<K,V> Clone<K,V>(IDictionary<K,V> dict)
        {
            if (dict == null) return null;
            Dictionary<K, V> clone = new Dictionary<K, V>(dict.Count);
            dict.CopyTo(clone.ToArray(), 0);
            return clone;
        }

        public static StringDictionary FilterProperties(StringDictionary properties, string optionPrefix)
        {
            if (properties == null)
            {
                throw new ArgumentNullException(nameof(properties), "The given properties object was null.");
            }

            StringDictionary filtered = new StringDictionary();
            foreach (string entry in properties.Keys)
            {
                if (entry.StartsWith(optionPrefix))
                {
                    string name = entry.Substring(optionPrefix.Length);
                    filtered.Add(name, properties[entry]);
                }
            }

            return filtered;
        }
    }

#region abstract Property Interceptor classes

    internal abstract class PropertyInterceptor<T> : StringDictionary where T : class
    {
        protected delegate void ApplyProperty(T instance, string key, string value);
        protected delegate string GetProperty(T instance, string key);
        protected delegate void ClearProperty(T instance);
        protected delegate bool CheckProperty(T instance);

        protected struct Interceptor
        {
            public ApplyProperty Setter;
            public GetProperty Getter;
            public ClearProperty Reset;
            public CheckProperty Exists;
        }


        private readonly StringDictionary properties;
        private readonly IDictionary<string, Interceptor> interceptors;
        private readonly T instance;
        protected PropertyInterceptor(T instance, StringDictionary properties, IDictionary<string, Interceptor> interceptors) : base()
        {
            this.properties = properties;
            this.instance = instance;

            // initialize interceptor map
            this.interceptors = new Dictionary<string, Interceptor>();
            foreach (string key in interceptors.Keys)
            {
                AddInterceptor(key, interceptors[key]);
            }

        }

        protected T Instance { get { return instance; } }

        protected StringDictionary Properties { get { return properties; } }

        protected void AddInterceptor(string key, Interceptor interceptor)
        {
            bool updated = false;
            // add new interceptor 
            if (!interceptors.ContainsKey(key))
            {
                this.interceptors.Add(key, interceptor);
            }
            else
            {
                // update interceptor
                // this allows subs classes to override base classes.
                this.interceptors[key] = interceptor;
                updated = true;
            }
            // Remove intercepted properties from base string dictionary
            if (properties.ContainsKey(key) || updated)
            {
                SetProperty(key, properties[key]);
                properties.Remove(key);
            }
        }

        protected void AddProperty(string key, string value)
        {
            if (interceptors.ContainsKey(key))
            {
                interceptors[key].Setter(instance, key, value);
            }
            else
            {
                properties.Add(key, value);
            }
        }

        protected void SetProperty(string key, string value)
        {
            if (interceptors.ContainsKey(key))
            {
                interceptors[key].Setter(instance, key, value);
            }
            else
            {
                properties[key] = value;
            }
        }

        protected string GetValue(string key)
        {
            string value = null;
            if (interceptors.ContainsKey(key))
            {
                if (interceptors[key].Exists(instance))
                {
                    value = interceptors[key].Getter(instance, key);
                }
            }
            else
            {
                value = properties[key];
            }
            return value;
        }

        protected int InterceptorCount
        {
            get
            {
                int count = 0;
                foreach (string key in interceptors.Keys)
                {
                    Interceptor i = interceptors[key];
                    if (i.Exists(instance))
                    {
                        count++;
                    }
                }
                return count;
            }
        }

        #region IDictionary<> Methods

        public override int Count => this.properties.Count + InterceptorCount;

        public override void Add(string key, string value)
        {
            AddProperty(key, value);
        }

        public override string this[string key]
        {
            get
            {
                return GetValue(key);
            }
            set
            {
                SetProperty(key, value);
            }
        }

        public override void Clear()
        {
            this.properties.Clear();
            Interceptor[] set = interceptors.Values.ToArray();
            foreach (Interceptor i in set)
            {
                i.Reset?.Invoke(Instance);
            }
        }

        public override void Remove(string key)
        {
            if (this.properties.ContainsKey(key))
            {
                this.properties.Remove(key);
            }
            else if (this.interceptors.ContainsKey(key))
            {
                if (this.interceptors[key].Exists(Instance))
                {
                    this.interceptors[key].Reset(Instance);
                }
            }
        }

        public override bool ContainsKey(string key)
        {
            if (this.properties.ContainsKey(key))
            {
                return true;
            }
            else if (this.interceptors.ContainsKey(key))
            {
                return this.interceptors[key].Exists(Instance);
            }
            return false;
        }

        public override ICollection Keys
        {
            get
            {
                ISet<string> keys = new HashSet<string>();
                foreach (string key in interceptors.Keys)
                {
                    Interceptor i = interceptors[key];
                    if (i.Exists(instance))
                    {
                        keys.Add(key);
                    }
                }
                foreach (string key in properties.Keys)
                {
                    keys.Add(key);
                }

                return keys.ToList();
            }
        }

        public override ICollection Values
        {
            get
            {
                ISet<object> values = new HashSet<object>();
                foreach (string key in interceptors.Keys)
                {
                    Interceptor i = interceptors[key];
                    if (i.Exists(instance))
                    {
                        values.Add(i.Getter(instance, key));
                    }
                }
                foreach (object value in properties.Values)
                {
                    values.Add(value);
                }

                return values.ToList();
            }
        }

        #endregion
        public override string ToString()
        {
            string result = base.ToString();
            foreach (string key in Keys)
            {
                result += "\n" + key.ToString() + ": " + GetValue(key).ToString();
            }
            return result;
        }

    }

    #region RelfectionPropertyInterceptor

    internal abstract class ReflectionPropertyInterceptor<T> : PropertyInterceptor<T> where T : class
    {
        #region static methods

        private static bool DefaultExists(T instance)
        {
            return true;
        }
        
        private static void SetReflectedValue(PropertyInfo info, object instance, string value)
        {
            if (info.GetSetMethod() != null)
            {
                object objValue = ParseReflectedValue(info, value);
                info.SetValue(instance, objValue, null);
            }
        }

        private static object GetReflectedValue(PropertyInfo info, object instance)
        {
            object value = null;
            if (info.GetGetMethod() != null)
            {
                value = info.GetValue(instance);
            }
            return value;
        }

        private static object ParseReflectedValue(PropertyInfo info, string value)
        {
            Type targetType = info.PropertyType;
            return PropertyUtil.ConvertType(targetType, value);
        }

        #endregion

        protected struct ReflectedInteceptor
        {
            public PropertyInfo ReflectedProperty;
            public Interceptor BaseInterceptor;
        }


        private Dictionary<string, ReflectedInteceptor> autoInterceptors = null;

        #region Contructor

        protected ReflectionPropertyInterceptor(T instance, StringDictionary properties, IDictionary<string, Interceptor> interceptors ) : base(instance, properties, interceptors)
        {
            this.autoInterceptors = CreateReflectionInterceptors(instance);
            if (this.autoInterceptors != null)
            {
                foreach (string propertyName in this.autoInterceptors.Keys)
                {
                    this.AddInterceptor(propertyName, this.autoInterceptors[propertyName].BaseInterceptor);
                }

                if (Tracer.IsDebugEnabled)
                {
                    StringBuilder sb = new StringBuilder();
#if TRACE
                    sb.AppendFormat("Generated {0} reflection properties, Properties = [", this.autoInterceptors.Keys.Count);
                    foreach (string propertyName in this.autoInterceptors.Keys)
                    {
                        sb.AppendFormat("\n\t{0},\n", propertyName);
                    }
                    sb.AppendFormat("]");
#else
                    sb.AppendFormat("Generated {0} reflection properties", this.autoInterceptors.Keys.Count);
#endif
                    Tracer.DebugFormat(sb.ToString());
                }
                
            }
        }

        #endregion

        #region abstract Properties

        protected abstract string PropertyPrefix { get; }

        #endregion

        protected abstract bool CanReflect(PropertyInfo reflectedProperty);

        #region Private Methods

        private Interceptor? CreateReflectedInterceptor(PropertyInfo reflectedProperty)
        {
            Interceptor? result = null;

            if (reflectedProperty.GetSetMethod() == null)
            {

            }
            else
            {


                ApplyProperty reflectedSetter = (inst, key, value) =>
                {
                    SetReflectedValue(reflectedProperty, inst, value);
                };

                GetProperty reflectedGetter = (inst, key) =>
                {
                    return GetReflectedValue(reflectedProperty, inst)?.ToString();
                };

                ClearProperty reflectedClear = (inst) =>
                {
                    SetReflectedValue(reflectedProperty, inst, null);
                };

                CheckProperty reflectedExists = DefaultExists;

                result = new Interceptor()
                {
                    Getter = reflectedGetter,
                    Setter = reflectedSetter,
                    Reset = reflectedClear,
                    Exists = reflectedExists
                };
            }

            return result;
        }

        private Dictionary<string, ReflectedInteceptor> CreateReflectionInterceptors(T instance)
        {
            Dictionary<string, PropertyInfo> objProperties = PropertyUtil.GetPropertiesForClass(instance);

            Dictionary<string, ReflectedInteceptor> result = new Dictionary<string, ReflectedInteceptor>();

            if (Tracer.IsDebugEnabled)
            {

                List<string> stringPropertyNames = new List<string>(objProperties.Keys.Count);
                foreach (string pName in objProperties.Keys)
                {
                    string propertyName = this.PropertyPrefix + pName;
                    stringPropertyNames.Add(propertyName);
                }

                Tracer.DebugFormat("Creating reflection interceptors for Class instance {0}, Generating Properties = {1}", instance.GetType().Name, PropertyUtil.ToString(stringPropertyNames));
            }

            foreach (string key in objProperties.Keys)
            {
                string propertyName = this.PropertyPrefix + key;
                PropertyInfo info = objProperties[key];
                if (!CanReflect(info)) continue;
                //MethodInfo propGetter = info.GetGetMethod();
                Interceptor? reflectedInterceptor = CreateReflectedInterceptor(info);
                if (reflectedInterceptor != null)
                {
                    Interceptor i = (Interceptor)reflectedInterceptor;
                    ReflectedInteceptor ri = new ReflectedInteceptor()
                    {
                        ReflectedProperty = info,
                        BaseInterceptor = i,
                    };
                    result.Add(propertyName, ri);
                }

            }

            return result;
        }

        #endregion

    }

    #endregion // end abstract reflection interception

    #endregion

#region Transport Property Interceptor class 

    internal class SecureTransportPropertyInterceptor : TransportPropertyInterceptor
    {
        internal const string SSL_PROTOCOLS_PROPERTY = "transport." + "SSLProtocol";
        
        private static bool ValidateProtocolString(string protocolString, out string cleanProtocols)
        {
            const string COMMA = ",";
            cleanProtocols = protocolString;
            if (protocolString != null)
            {
                string trim = protocolString.Trim();
                if(trim.StartsWith(COMMA) || trim.EndsWith(COMMA))
                {
                    return false;
                }
                cleanProtocols = trim;
            }
            return true;
        }

        protected static Dictionary<string, Interceptor> secureTransportConextInterceptors = new Dictionary<string, Interceptor>()
        {
            {
                SSL_PROTOCOLS_PROPERTY,
                new Interceptor()
                {
                    Getter = (context, key) =>
                    {
                        return (context as ISecureTransportContext).SSLProtocol;
                    },
                    Setter = (context, key, value) =>
                    {
                        string cleanValue;
                        if(!ValidateProtocolString(value, out cleanValue))
                        {
                            throw new InvalidPropertyException(key, "Protocol string can not start or end with ','");
                        }
                        else
                        {
                            (context as ISecureTransportContext).SSLProtocol = cleanValue;
                        }
                    },
                    Exists = (context) => 
                    {
                        return true;
                    },
                    Reset = (context) =>
                    {
                        (context as ISecureTransportContext).SSLProtocol = null;
                    }
                }

            }
        };
        
        
        public SecureTransportPropertyInterceptor(ISecureTransportContext context, StringDictionary props ) : base(context, props)
        {


            foreach(string key in secureTransportConextInterceptors.Keys)
            {
                this.AddInterceptor(key, secureTransportConextInterceptors[key]);
            }
        }

        protected override bool CanReflect(PropertyInfo reflectedProperty)
        {
            Type TargetType = reflectedProperty.PropertyType;
            return TargetType.IsPrimitive || TargetType.Equals(typeof(string));
        }

    }
    
    internal class TransportPropertyInterceptor : ReflectionPropertyInterceptor<ITransportContext>
    {
        protected static Dictionary<string, Interceptor> transportContextInterceptors = new Dictionary<string, Interceptor>()
        {

        };
        
        public TransportPropertyInterceptor(ITransportContext c, StringDictionary transportProperties) : base(c, transportProperties, transportContextInterceptors)
        {
        
        }

        protected override string PropertyPrefix => "transport.";

        protected override bool CanReflect(PropertyInfo reflectedProperty)
        {
            return true;
        }

    }

#endregion
}
