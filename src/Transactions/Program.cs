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
using Apache.NMS.AMQP;

namespace Transactions
{
    static class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("NMS-AMQP Transactions Example");

            var connectionFactory = new NmsConnectionFactory("admin", "admin", "amqp://127.0.0.1:5672");
            var connection = connectionFactory.CreateConnection();
            connection.ClientId = "TransactionsExampleSender";

            var session = connection.CreateSession(AcknowledgementMode.Transactional);
            var queue = session.GetQueue("TransactionQueue");
            var producer = session.CreateProducer(queue);

            for (int i = 1; i <= 5; i++)
            {
                ITextMessage message = producer.CreateTextMessage($"Message  {i}");
                producer.Send(message);
                Console.WriteLine("Sent message " + i);
            }

            session.Rollback();
            Console.WriteLine("Rollback");

            for (int i = 6; i <= 10; i++)
            {
                ITextMessage message = producer.CreateTextMessage($"Message  {i}");
                producer.Send(message);
                Console.WriteLine("Sent message " + i);
            }

            session.Commit();

            var consumer = session.CreateConsumer(queue);

            connection.Start();

            for (int i = 0; i < 5; i++)
            {
                var message = consumer.Receive() as ITextMessage;
                Console.WriteLine("Message " + message.Text + " received");
            }

            Console.ReadKey();

            producer.Close();
            session.Close();
            connection.Close();

            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }
    }
}