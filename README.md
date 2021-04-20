# Apache-NMS-AMQP

## Build Status

OS | Status
---|---
Linux | [![Build Status](https://travis-ci.org/apache/activemq-nms-amqp.svg?branch=master)](https://travis-ci.org/apache/activemq-nms-amqp)
Windows | [![Build status](https://ci.appveyor.com/api/projects/status/yn2wkhq1nbhkfsur?svg=true)](https://ci.appveyor.com/project/ApacheSoftwareFoundation/activemq-nms-amqp)

### Installing Apache NMS AMQP

You can install [Apache NMS AMQP with NuGet](https://www.nuget.org/packages/Apache.NMS.AMQP):

```
Install-Package Apache.NMS.AMQP
```

Or via the .NET Core command line interface:

```
dotnet add package Apache.NMS.AMQP
```

Either commands, from Package Manager Console or .NET Core CLI, will download and install Apache NMS AMQP and all required dependencies.

## Overview
The goal of this project is to combine the [.NET Message Service API](http://activemq.apache.org/nms/) (NMS) with
the [Advanced Message Queuing Protocol (AMQP)](https://www.amqp.org/) 1.0 standard wireline protocol. Historically, the Apache community created the NMS API which provided a vendor agnostic .NET interface to a variety of messaging systems. The NMS API gives the flexibility to write .NET applications in C#, VB or any other .NET language, all while using a single API to connect to any number of messaging providers. The Advanced Message Queuing Protocol (AMQP) is an open and standardized internet protocol for reliably passing messages between applications or organizations.
Before AMQP became a standard, organizations used proprietary wireline protocols to connect their systems which lead to vendor lock-in and integration problems when integrating with external organizations.

The key to enabling vendor independence and mass adoption of technology is to combine open source APIs and standard wireline protocols which is precisely what this  project is all about.  Here's how AMQP 1.0 support within NMS helps the .NET community:
 - __More Choice:__ As more message brokers and services implement the AMQP 1.0 standard wireline, .NET developers and architects will have more options for messaging technology.
 - __No Migration Risk:__ Since AMQP 1.0 is a wireline standard, you won't run into the problems that used to happen when switching between implementations.
 - __Innovation:__ Competition is a key component of technology innovation. Directly competitive messaging implementations, with seamless pluggability, forces vendors to innovate and differentiate.
 
If you are a .NET developer that doesn't want to be locked into a messaging implementation then get engaged with this project. Here you will find the open source code base and please provide comments and make your own enhancements. The project will be folded into the Apache community once fully mature.


## AMQP1.0 Protocol Engine AmqpNetLite
Apache-NMS-AMQP uses [AmqpNetLite](https://github.com/Azure/amqpnetlite) as the underlying AMQP 1.0 transport Protocol engine. 

## Overall Architecture
Apache-NMS-AMQP should bridge the familiar NMS concepts to AMQP protocol concepts as described in the document [amqp-bindmap-jms-v1.0-wd09.pdf](https://www.oasis-open.org/committees/download.php/60574/amqp-bindmap-jms-v1.0-wd09.pdf).
So in general most of the top level classes that implement the Apache.NMS interface _Connection, Session, MessageProducer,_ etc  create, manage, and destroy the amqpnetlite equivalent object _Connection, Session, Link,_ etc.

## Getting started
- [Configuration](docs/configuration.md) contains various uri options that can be set when defining a ConnectionFactory.
- [Interested in the code?](docs/working_with_code.md) Clone and build the projects.
- Want to contribute? Github pull requests are one way to contribute, but our real issue tracker is [JIRA](https://issues.apache.org/jira/issues/?jql=project%20%3D%20AMQNET%20AND%20component%20%3D%20AMQP).

## Amqp Provider NMS Feature Support

| Feature       | Supported | Comments         |
|---------------|:---------:|:-----------------|
| TLS/SSL       | Y | Configuration is supported using transport properties. |
| Client Certificate Authentication | Y | Configuration is supported using transport properties. |
| Transactions (AcknowledgementMode.Transactional) | Y |
| Distributed Transactions (INetTxConnectionFactory, INetTxConnection, INetTxSession) | N | |
| AcknowledgementMode.AutoAcknowledge | Y | |
| AcknowledgementMode.DupsOkAcknowledge | Y | |
| AcknowledgementMode.ClientAcknowledge | Y | |
| AcknowledgementMode.IndividualAcknowledge | Y | |
| IObjectMessage | Y * | Amqp value object bodies and dotnet serializable object bodies are supported. |
| IBytesMessage | Y | |
| IStreamMessage | Y | |
| IMapMessage | Y | |
| ITextMessage | Y | |
| IMessage | Y | |
| IConnectionFactory | Y | |
| IConnection | Y | The ConnectionInterruptedListener event and the ConnectionResumedListener are not supported. |
| ProducerTransformerDelegate | N | Any member access should throw a NotSupportedException. |
| ConsumerTransformerDelegate | N | Any member access should throw a NotSupportedException. |
| ISession | Y | |
[ INMSContext | Y | |
| IQueue | Y | |
| ITopic | Y | |
| ITemporaryQueue | Y | |
| ITemporaryTopic | Y | |
| IMessageProducer | Y * | Anonymous producers are only supported on connections with the ANONYMOUS-RELAY capability. |
| INMSProducer | Y | |
| MsgDeliveryMode.Persistent | Y | Producers will block on send until an outcome is received or will timeout after waiting the RequestTimeout timespan amount. Exceptions may be throw depending on the outcome or if the producer times out. |
| MsgDeliveryMode.NonPersistent | Y | Producers will not block on send nor expect to receive an outcome. Should an exception be raised from the outcome the exception will be delivered using the the connection ExceptionListener. |
| IMessageConsumer | Y | |
| INMSConsumer | Y | |
| Durable Consumers | Y | |
| Shared Consumers | Y | |
| IQueueBrowser | Y | |
| Configurable NMSMessageID and amqp serializtion | N | For future consideration. The prodiver will generate a MessageID from a sequence and serialize it as a string. |
| Flow control configuration | N | For future consideration. The provider will use amqpnetlite defaults except for initial link credits which is 200. |
| Object Deserialization Policy | N | For future consideration. The provider considers all Dotnet serialized objects in Object Message bodies are safe to deserialize. |
| Failover | Y

# Licensing 

This software is licensed under the terms you may find in the file named "LICENSE" in this directory.
