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

using Apache.NMS;
using NLog;

namespace NMS.AMQP.Test.TestAmqp
{
    class NLogAdapter : ITrace
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        public bool IsDebugEnabled => Logger.IsDebugEnabled;

        public bool IsErrorEnabled => Logger.IsErrorEnabled;

        public bool IsFatalEnabled => Logger.IsFatalEnabled;

        public bool IsInfoEnabled => Logger.IsInfoEnabled;

        public bool IsWarnEnabled => Logger.IsWarnEnabled;

        public void Debug(string message) => Logger.Debug(message);

        public void Error(string message) => Logger.Error(message);

        public void Fatal(string message) => Logger.Fatal(message);

        public void Info(string message) => Logger.Info(message);

        public void Warn(string message) => Logger.Warn(message);
    }
}