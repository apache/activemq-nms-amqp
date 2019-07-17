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
using System.Collections.Specialized;
using Apache.NMS;
using Apache.NMS.AMQP;
using CommandLine;

namespace StructuredMessage
{
    class CommandLineOpts
    {
        // URI for message broker. Must be of the format amqp://<host>:<port> or amqps://<host>:<port>
        [Option("uri", Required = true, HelpText = "The URI for the AMQP Message Broker")]
        public string host { get; set; }
        // Connection Request Timeout
        [Option("ct", Default = 15000, HelpText = "the connection request timeout in milliseconds.")]
        public long connTimeout { get; set; }
        // UserName for authentication with the broker.
        [Option("cu", Default = null, HelpText = "The Username for authentication with the message broker")]
        public string username { get; set; }
        // Password for authentication with the broker
        [Option("cpwd", Default = null, HelpText = "The password for authentication with the message broker")]
        public string password { get; set; }
        [Option("cid", Default = null, HelpText = "The Client ID on the connection")]
        public string clientId { get; set; }
        // Logging Level
        [Option("log", Default = "warn", HelpText = "Sets the log level for the application and NMS Library. The levels are (from highest verbosity): debug,info,warn,error,fatal.")]
        public string logLevel { get; set; }
        //
        [Option("topic", Default = null, HelpText = "Topic to publish messages to. Can not be used with --queue.")]
        public string topic { get; set; }
        //
        [Option("queue", Default = null, HelpText = "Queue to publish messages to. Can not be used with --topic.")]
        public string queue { get; set; }
        //
        [Option("deliveryMode", Default = 5, HelpText = "Message Delivery Mode, Persistnent(0) and Non Persistent(1). The default is Persistent(0).")]
        public int mode { get; set; }
    }
    class Program
    {
        
        private static void RunWithOptions (CommandLineOpts opts)
        {        
            ITrace logger = new Logger(Logger.ToLogLevel(opts.logLevel));
            Tracer.Trace = logger;

            string ip = opts.host;
            Uri providerUri = new Uri(ip);
            Console.WriteLine("scheme: {0}", providerUri.Scheme);

            IConnection conn = null;
            if (opts.topic == null && opts.queue == null)
            {
                Console.WriteLine("ERROR: Must specify a topic or queue destination");
                return;
            }
            try
            {
                NmsConnectionFactory factory = new NmsConnectionFactory(ip);
                if (opts.username != null)
                {
                    factory.UserName = opts.username;
                }
                if (opts.password != null)
                {
                    factory.Password = opts.password;
                }
                if (opts.clientId != null)
                {
                    factory.ClientId = opts.clientId;
                }

                if (opts.connTimeout != default)
                {
                    factory.SendTimeout = opts.connTimeout;
                }

                Console.WriteLine("Creating Connection...");
                conn = factory.CreateConnection();
                conn.ExceptionListener += (logger as Logger).LogException;
                Console.WriteLine("Created Connection.");
                Console.WriteLine("Version: {0}", conn.MetaData);
                Console.WriteLine("Creating Session...");
                ISession ses = conn.CreateSession();
                Console.WriteLine("Session Created.");

                conn.Start();
                IDestination dest = (opts.topic==null) ? (IDestination)ses.GetQueue(opts.queue) : (IDestination)ses.GetTopic(opts.topic);
                Console.WriteLine("Creating Message Producer for : {0}...", dest);
                IMessageProducer prod = ses.CreateProducer(dest);
                IMessageConsumer consumer = ses.CreateConsumer(dest);
                Console.WriteLine("Created Message Producer.");
                prod.DeliveryMode = opts.mode == 0 ? MsgDeliveryMode.NonPersistent : MsgDeliveryMode.Persistent;
                prod.TimeToLive = TimeSpan.FromSeconds(20);
                IMapMessage mapMsg = prod.CreateMapMessage();
                IStreamMessage streamMsg = prod.CreateStreamMessage();

                Console.WriteLine("Starting Connection...");
                conn.Start();
                Console.WriteLine("Connection Started: {0} Resquest Timeout: {1}", conn.IsStarted, conn.RequestTimeout);
                Tracer.InfoFormat("Sending MapMsg");
                // Map Msg Body 
                mapMsg.Body.SetString("mykey", "Hello World!");
                mapMsg.Body.SetBytes("myBytesKey", new byte[] { 0x6d, 0x61, 0x70 });
                Console.WriteLine("Sending Msg: {0}", mapMsg.ToString());
                prod.Send(mapMsg);
                mapMsg.ClearBody();

                // Stream  Msg Body
                streamMsg.WriteBytes(new byte[] { 0x53, 0x74, 0x72});
                streamMsg.WriteInt64(1354684651565648484L);
                streamMsg.WriteObject("bar");
                streamMsg.Properties["foobar"] = 42 + "";
                Console.WriteLine("Sending Msg: {0}", streamMsg.ToString());
                prod.Send(streamMsg);
                streamMsg.ClearBody();

                IMessage rmsg = null;
                for (int i = 0; i < 2; i++)
                {
                    Tracer.InfoFormat("Waiting to receive message {0} from consumer.", i);
                    rmsg = consumer.Receive(TimeSpan.FromMilliseconds(opts.connTimeout));
                    if(rmsg == null)
                    {
                        Console.WriteLine("Failed to receive Message in {0}ms.", opts.connTimeout);
                    }
                    else
                    {
                        Console.WriteLine("Received Message with id {0} and contents {1}.", rmsg.NMSMessageId, rmsg.ToString());
                        foreach (string key in rmsg.Properties.Keys)
                        { 
                            Console.WriteLine("Message contains Property[{0}] = {1}", key, rmsg.Properties[key].ToString());
                        }
                    }    
                }
                if (conn.IsStarted)
                {
                    Console.WriteLine("Closing Connection...");
                    conn.Close();
                    Console.WriteLine("Connection Closed.");
                }
            }
            catch(NMSException ne)
            {
                Console.WriteLine("Caught NMSException : {0} \nStack: {1}", ne.Message, ne);
            }
            catch (Exception e)
            {
                Console.WriteLine("Caught unexpected exception : {0}", e);
            }
            finally
            {
                if(conn != null)
                {
                    conn.Dispose();
                }
            }
        }
    
        static void Main(string[] args)
        {
            CommandLineOpts opts = new CommandLineOpts();
            ParserResult<CommandLineOpts> result = CommandLine.Parser.Default.ParseArguments<CommandLineOpts>(args)
                .WithParsed<CommandLineOpts>(options => RunWithOptions(options));
        }
    }

    #region Logging

    class Logger : ITrace
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
            if(logString == null || logString.Length == 0)
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

        public void LogException(Exception ex)
        {
            this.Warn("Exception: "+ex.Message);
        }

        public Logger() : this(LogLevel.WARN)
        {
        }

        public Logger(LogLevel lvl)
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
            if(IsDebugEnabled)
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
    #endregion 
}
