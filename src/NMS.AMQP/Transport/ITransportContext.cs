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

using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;

namespace Apache.NMS.AMQP.Transport
{
    public interface ITransportContext
    {
        int ReceiveBufferSize { get; set; }

        int ReceiveTimeout { get; set; }

        int SendBufferSize { get; set; }

        int SendTimeout { get; set; }
        bool TcpNoDelay { get; set; }

        bool UseLogging { get; set; }
        bool IsSecure { get; }

        ITransportContext Copy();

        Task<Amqp.Connection> CreateAsync(Address address, Open open = null, OnOpened onOpened = null);
    }
}
