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
using Apache.NMS;
using Amqp;

namespace Apache.NMS.AMQP.Test.Util
{

    class NMSLogger : ITrace
    {
        public enum LogLevel
        {
            OFF = -1,
            FATAL,
            ERROR,
            WARN,
            INFO,
            DEBUG
        }

        public static LogLevel ToLogLevel(string logString)
        {
            if (logString == null || logString.Length == 0)
            {
                return LogLevel.OFF;
            }
            if ("FATAL".StartsWith(logString, StringComparison.CurrentCultureIgnoreCase))
            {
                return LogLevel.FATAL;
            }
            else if ("ERROR".StartsWith(logString, StringComparison.CurrentCultureIgnoreCase))
            {
                return LogLevel.ERROR;
            }
            else if ("WARN".StartsWith(logString, StringComparison.CurrentCultureIgnoreCase))
            {
                return LogLevel.WARN;
            }
            else if ("INFO".StartsWith(logString, StringComparison.CurrentCultureIgnoreCase))
            {
                return LogLevel.INFO;
            }
            else if ("DEBUG".StartsWith(logString, StringComparison.CurrentCultureIgnoreCase))
            {
                return LogLevel.DEBUG;
            }
            else
            {
                return LogLevel.OFF;
            }
        }

        private LogLevel lv;

        public NMSLogger(LogLevel lvl)
        {
            lv = lvl;
        }

        public bool IsDebugEnabled
        {
            get
            {
                return lv >= LogLevel.DEBUG;
            }
        }

        public bool IsErrorEnabled
        {
            get
            {

                return lv >= LogLevel.ERROR;
            }
        }

        public bool IsFatalEnabled
        {
            get
            {
                return lv >= LogLevel.FATAL;
            }
        }

        public bool IsInfoEnabled
        {
            get
            {
                return lv >= LogLevel.INFO;
            }
        }

        public bool IsWarnEnabled
        {
            get
            {
                return lv >= LogLevel.WARN;
            }
        }

        public void Debug(string message)
        {
            if (IsDebugEnabled)
                Console.WriteLine("Debug: {0}", message);
        }

        public void Error(string message)
        {
            if (IsErrorEnabled)
                Console.WriteLine("Error: {0}", message);
        }

        public void Fatal(string message)
        {
            if (IsFatalEnabled)
                Console.WriteLine("Fatal: {0}", message);
        }

        public void Info(string message)
        {
            if (IsInfoEnabled)
                Console.WriteLine("Info: {0}", message);
        }

        public void Warn(string message)
        {
            if (IsWarnEnabled)
                Console.WriteLine("Warn: {0}", message);
        }
    }
}
