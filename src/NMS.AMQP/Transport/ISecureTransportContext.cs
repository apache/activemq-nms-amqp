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

using System.Net.Security;

namespace Apache.NMS.AMQP.Transport
{
    public interface ISecureTransportContext : ITransportContext
    {
        string ServerName { get; set; }

        string ClientCertFileName { get; set; }

        string ClientCertSubject { get; set; }

        string ClientCertPassword { get; set; }

        string KeyStoreName { get; set; }
        
        string KeyStoreLocation { get; set; }

        bool AcceptInvalidBrokerCert { get; set; }

        string SSLProtocol { get; set; }
        
        RemoteCertificateValidationCallback ServerCertificateValidateCallback { get; set; }

        LocalCertificateSelectionCallback ClientCertificateSelectCallback { get; set; }

        bool CheckCertificateRevocation { get; set; }
        
    }
}
