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
using System.Diagnostics;
using Apache.NMS;
using Apache.NMS.Util;
using Apache.NMS.Amqp;

namespace Apache.NMS.ActiveMQ.Test
{
    public class TestMain
    {
        public static void HelloWorld_amqpBroker_0_10Protocol(string[] args)
        {
            // Connect to a qpid broker on localhost:5672
            // Run HelloWorld using amqp0-10 protocol
            //
            // Notes:
            //  * Run qpidd on localhost
            //  * Ensure the nmsprovider-amqp.config file exists 
            //    in the executable folder (build\net4-0\debug).
            //  * Ensure the unmanaged qpid*.dll and boost*.dll files from 
            //      .nant\library\local\org.apache.qpid\Apache.Qpid\<version>\net-4.0\debug
            //      are in project's Output Path (build\net-4.0\debug) so that they may be
            //      loaded by org.apache.qpid.messaging.dll.

            Uri connecturi = new Uri("amqp:localhost:5672");

            IConnectionFactory factory = new NMSConnectionFactory(connecturi, "Bob", "reconnect-timeout:5", "protocol:amqp0-10");

            using (IConnection connection = factory.CreateConnection())
            using (ISession session = connection.CreateSession())
            {
                IDestination destination = SessionUtil.GetDestination(session, "amq.topic");

                // Create a consumer and producer
                using (IMessageConsumer consumer = session.CreateConsumer(destination))
                using (IMessageProducer producer = session.CreateProducer(destination))
                {
                    // Start the connection so that messages will be processed.
                    connection.Start();

                    // Create and send a message
                    ITextMessage request = session.CreateTextMessage("Hello World!");
                    request.Properties["NMSXGroupID"] = "cheese";
                    request.Properties["myHeader"] = "Cheddar";

                    producer.Send(request);

                    //// Consume a message
                    ITextMessage message = consumer.Receive() as ITextMessage;
                    if (message == null)
                    {
                        Console.WriteLine("No message received!");
                    }
                    else
                    {
                        // Expected output:
                        //  Received message text: Hello World!
                        //  Received message properties: {x-amqp-0-10.routing-key=, NMSXGroupID=cheese, myHeader=Cheddar}
                        Console.WriteLine("Received message text: " + message.Text);
                        Console.WriteLine("Received message properties: " + message.Properties.ToString());
                    }
                    connection.Stop();
                }
            }
        }
        public static void Main(string[] args)
        {
            HelloWorld_amqpBroker_0_10Protocol(args);
        }
    }
}