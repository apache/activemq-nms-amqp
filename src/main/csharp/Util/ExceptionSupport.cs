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
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using Apache.NMS;
using Amqp.Framing;
using Amqp;

namespace Apache.NMS.AMQP.Util
{
    class ExceptionSupport
    {
        private static readonly Dictionary<string, Type> errTypeMap;
        
        static ExceptionSupport()
        {
            // mapping of amqp .Net Lite error code to NMS exception type

            errTypeMap = new Dictionary<string, Type>();
            errTypeMap.Add(NMSErrorCode.CONNECTION_TIME_OUT, typeof(NMSConnectionException));
            errTypeMap.Add(ErrorCode.ConnectionRedirect, typeof(NMSConnectionException));
            errTypeMap.Add(ErrorCode.ConnectionForced, typeof(NMSConnectionException));
            errTypeMap.Add(ErrorCode.IllegalState, typeof(IllegalStateException));

            errTypeMap.Add(NMSErrorCode.INTERNAL_ERROR, typeof(NMSException));
            errTypeMap.Add(NMSErrorCode.UNKNOWN_ERROR, typeof(NMSException));
            errTypeMap.Add(NMSErrorCode.SESSION_TIME_OUT, typeof(NMSException));
            errTypeMap.Add(NMSErrorCode.LINK_TIME_OUT, typeof(NMSException));
            errTypeMap.Add(ErrorCode.DecodeError, typeof(NMSException));
            errTypeMap.Add(ErrorCode.DetachForced, typeof(NMSException));
            errTypeMap.Add(ErrorCode.ErrantLink, typeof(NMSException));
            errTypeMap.Add(ErrorCode.FrameSizeTooSmall, typeof(NMSConnectionException));
            errTypeMap.Add(ErrorCode.FramingError, typeof(NMSConnectionException));
            errTypeMap.Add(ErrorCode.HandleInUse, typeof(NMSException));
            errTypeMap.Add(ErrorCode.InternalError, typeof(NMSException));
            errTypeMap.Add(ErrorCode.InvalidField, typeof(NMSException));
            errTypeMap.Add(ErrorCode.LinkRedirect, typeof(NMSException));
            errTypeMap.Add(ErrorCode.MessageReleased, typeof(IllegalStateException));
            errTypeMap.Add(ErrorCode.MessageSizeExceeded, typeof(NMSException));
            errTypeMap.Add(ErrorCode.NotAllowed, typeof(NMSException));
            errTypeMap.Add(ErrorCode.NotFound, typeof(InvalidDestinationException));
            errTypeMap.Add(ErrorCode.NotImplemented, typeof(NMSException));
            errTypeMap.Add(ErrorCode.PreconditionFailed, typeof(NMSException));
            errTypeMap.Add(ErrorCode.ResourceDeleted, typeof(NMSException));
            errTypeMap.Add(ErrorCode.ResourceLimitExceeded, typeof(NMSException));
            errTypeMap.Add(ErrorCode.ResourceLocked, typeof(NMSException));
            errTypeMap.Add(ErrorCode.Stolen, typeof(NMSException));
            errTypeMap.Add(ErrorCode.TransactionRollback, typeof(TransactionRolledBackException));
            errTypeMap.Add(ErrorCode.TransactionTimeout, typeof(TransactionInProgressException));
            errTypeMap.Add(ErrorCode.TransactionUnknownId, typeof(TransactionRolledBackException));
            errTypeMap.Add(ErrorCode.TransferLimitExceeded, typeof(NMSException));
            errTypeMap.Add(ErrorCode.UnattachedHandle, typeof(NMSException));
            errTypeMap.Add(ErrorCode.UnauthorizedAccess, typeof(NMSSecurityException));
            errTypeMap.Add(ErrorCode.WindowViolation, typeof(NMSException));
            
        }

        private static FieldInfo[] GetConstants(Type type)
        {
            ArrayList list = new ArrayList();
            FieldInfo[] fields = type.GetFields(
                BindingFlags.Static | 
                BindingFlags.Public | 
                BindingFlags.FlattenHierarchy
                );
            foreach (FieldInfo field in fields)
            {
                if (field.IsLiteral && !field.IsInitOnly)
                    list.Add(field);
            }
            return (FieldInfo[])list.ToArray(typeof(FieldInfo));
        }

        private static string[] GetStringConstants(Type type)
        {
            FieldInfo[] fields = GetConstants(type);
            ArrayList list = new ArrayList(fields.Length);
            foreach(FieldInfo fi in fields)
            {
                if (fi.FieldType.Equals(typeof(string)))
                {
                    list.Add(fi.GetValue(null));
                }
            }
            return (string[])list.ToArray(typeof(string));
        }

        public static NMSException GetTimeoutException(IAmqpObject obj, string format, params object[] args)
        {
            return GetTimeoutException(obj, string.Format(format, args));
        }

        public static NMSException GetTimeoutException(IAmqpObject obj, string message)
        {
            Error e = null;
            if (obj is Amqp.Connection)
            {
                e = NMSError.CONNECTION_TIMEOUT;
            }
            else if (obj is Amqp.Session)
            {
                e = NMSError.SESSION_TIMEOUT;
            }
            else if (obj is Amqp.Link)
            {
                e = NMSError.LINK_TIMEOUT;
            }
            
            return GetException(e, message);

        }

        public static NMSException GetException(IAmqpObject obj, string format, params object[] args)
        {
            return GetException(obj, string.Format(format, args));
        }

        public static NMSException GetException(Error amqpErr, string format, params object[] args)
        {
            return GetException(amqpErr, string.Format(format, args));
        }

        public static NMSException GetException(IAmqpObject obj, string message="")
        {
            return GetException(obj.Error, message);
        }

        public static NMSException GetException(Error amqpErr, string message = "", Exception e = null)
        {
            string errCode = null;
            string errMessage = null;
            string additionalErrInfo = null;
            if (amqpErr == null)
            {
                amqpErr = NMSError.INTERNAL;
            }

            errCode = amqpErr.Condition.ToString();
            errMessage = amqpErr.Description;

            errMessage = errMessage != null ? ", Description = " + errMessage : "";

            if (amqpErr.Info != null && amqpErr.Info.Count > 0)
            {
                additionalErrInfo = ", ErrorInfo = " + Types.ConversionSupport.ToString(amqpErr.Info);
            }
            if (null == e)
            {
                // no exception given, create a NMSunthrownException to hold the 
                // stack and use it as the innerException in the constructors for
                // the NMSexception we create., the custom StackTrace() will allow exception listeners to
                // see the stack to here
                e = new NMSProviderError(errCode, errMessage);
            }
            NMSException ex = null;
            Type exType = null;
            if(errTypeMap.TryGetValue(errCode, out exType))
            {
                ConstructorInfo ci = exType.GetConstructor(new[] { typeof(string), typeof(string), typeof(Exception) });
                object inst = ci.Invoke(new object[] { message + errMessage + additionalErrInfo , errCode, e });
                ex = inst as NMSException;
            }
            else
            {
                ex = new NMSException(message + errMessage + additionalErrInfo, errCode, e);
            }
            return ex;
            
        }
        
        public static NMSException Wrap(Exception e, string format, params object[] args)
        {
            return Wrap(e, string.Format(format, args));
        }

        public static NMSException Wrap(Exception e, string message = "")
        {
            if(e == null)
            {
                return null;
            }
            NMSException nmsEx = null;
            string exMessage = message;
            if (exMessage == null || exMessage.Length == 0)
            {
                exMessage = e.Message;
            }
            if (e is NMSException)
            {
                return e as NMSException;
            }
            else if (e is AmqpException)
            {
                Error err = (e as AmqpException).Error;
                nmsEx = GetException(err, message, e);
                Tracer.DebugFormat("Encoutered AmqpException {0} and created NMS Exception {1}.", e, nmsEx);
            }
            else
            {
                nmsEx = new NMSException(exMessage, NMSErrorCode.INTERNAL_ERROR, e);
            }
            
            return nmsEx;
        }

    }

    #region Exceptions


    public class InvalidPropertyException : NMSException
    {
        protected static string ExFormat = "Invalid Property {0}. Cause: {1}";

        public InvalidPropertyException(string property, string message) : base(string.Format(ExFormat, property, message))
        {
            exceptionErrorCode = NMSErrorCode.PROPERTY_ERROR;
        }
    }
    // The API converts AMPQ Errors to NMSProviderError. This is typically added to
    // the Exception queue and passed to the ExceptionListener.  As the Exception is
    // instantiated but never thrown, it does not have an exception-stack trace. We add
    // one to the private member InstanceTrace and override StackTrace so useful 
    // information can be displayed.

    internal class NMSProviderError : NMSException
    {
        private string InstanceTrace;

        public NMSProviderError() : base()
        {
            System.Diagnostics.StackTrace trace = new System.Diagnostics.StackTrace(1, true);
            InstanceTrace = trace.ToString();
        }

        public NMSProviderError(string message) : base(message)
        {
            System.Diagnostics.StackTrace trace = new System.Diagnostics.StackTrace(1, true);
            InstanceTrace = trace.ToString();
        }

        public NMSProviderError(string message, string errorCode) : base(message, errorCode)
        {
            System.Diagnostics.StackTrace trace = new System.Diagnostics.StackTrace(1, true);
            InstanceTrace = trace.ToString();
        }

        public NMSProviderError(string message, NMSException innerException) : base(message, innerException)
        {
            System.Diagnostics.StackTrace trace = new System.Diagnostics.StackTrace(1, true);
            InstanceTrace = trace.ToString();
            exceptionErrorCode = innerException.ErrorCode ?? NMSErrorCode.INTERNAL_ERROR;
        }

        public NMSProviderError(string message, Exception innerException) : base(message, innerException)
        {
            System.Diagnostics.StackTrace trace = new System.Diagnostics.StackTrace(1, true);
            InstanceTrace = trace.ToString();
        }

        public NMSProviderError(string message, string errorCode, Exception innerException) : base(message, errorCode, innerException)
        {
            System.Diagnostics.StackTrace trace = new System.Diagnostics.StackTrace(1, true);
            InstanceTrace = trace.ToString();
        }

        public override string StackTrace
        {
            get
            {
                string stack = base.StackTrace;
                if (stack == null || stack.Length == 0)
                {
                    stack = InstanceTrace;
                }
                if (InnerException != null && (InnerException.StackTrace != null && InnerException.StackTrace.Length > 0))
                {
                    stack += "\nCause " + InnerException.Message + " : \n" +
                                 InnerException.StackTrace;

                }
                return stack;
            }
        }
    }

    #endregion

    #region Error Codes

    internal static class NMSError 
    {
        public static Error SESSION_TIMEOUT = new Error(NMSErrorCode.SESSION_TIME_OUT) { Description = "Session Begin Request has timed out." };
        public static Error CONNECTION_TIMEOUT = new Error(NMSErrorCode.SESSION_TIME_OUT) {  Description = "Connection Open Request has timed out." };
        public static Error LINK_TIMEOUT = new Error(NMSErrorCode.SESSION_TIME_OUT) { Description = "Link Attach Request has timed out." };
        public static Error PROPERTY = new Error(NMSErrorCode.PROPERTY_ERROR) {  Description = "Property Error." };
        public static Error UNKNOWN = new Error(NMSErrorCode.UNKNOWN_ERROR) { Description = "Unknown Error." };
        public static Error INTERNAL = new Error(NMSErrorCode.INTERNAL_ERROR) { Description = "Internal Error." };

    }
    internal static class NMSErrorCode
    {
        public static string CONNECTION_TIME_OUT = "nms:connection:timout";
        public static string SESSION_TIME_OUT = "nms:session:timout";
        public static string LINK_TIME_OUT = "nms:link:timeout";
        public static string PROPERTY_ERROR = "nms:property:error";
        public static string UNKNOWN_ERROR = "nms:unknown";
        public static string INTERNAL_ERROR = "nms:internal";
    }

    #endregion
}
