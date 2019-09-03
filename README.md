# Apache-NMS-AMQP
### Overview
The goal of this project is to combine the [.NET Message Service API](http://activemq.apache.org/nms/) (NMS) with
the [Advanced Message Queuing Protocol (AMQP)](https://www.amqp.org/) 1.0 standard wireline protocol. Historically, the Apache community created the NMS API which provided a vendor agnostic .NET interface to a variety of messaging systems. The NMS API gives the flexibility to write .NET applications in C#, VB or any other .NET language, all while using a single API to connect to any number of messaging providers. The Advanced Message Queuing Protocol (AMQP) is an open and standardized internet protocol for reliably passing messages between applications or organizations.
Before AMQP became a standard, organizations used proprietary wireline protocols to connect their systems which lead to vendor lock-in and integration problems when integrating with external organizations.

The key to enabling vendor independence and mass adoption of technology is to combine open source APIs and standard wireline protocols which is precisely what this  project is all about.  Here's how AMQP 1.0 support within NMS helps the .NET community:
 - __More Choice:__ As more message brokers and services implement the AMQP 1.0 standard wireline, .NET developers and architects will have more options for messaging technology.
 - __No Migration Risk:__ Since AMQP 1.0 is a wireline standard, you won't run into the problems that used to happen when switching between implementations.
 - __Innovation:__ Competition is a key component of technology innovation. Directly competitive messaging implementations, with seamless pluggability, forces vendors to innovate and differentiate.
 
If you are a .NET developer that doesn't want to be locked into a messaging implementation then get engaged with this project. Here you will find the open source code base and please provide comments and make your own enhancements. The project will be folded into the Apache community once fully mature.


### AMQP1.0 Protocol Engine AmqpNetLite
Apache-NMS-AMQP uses [AmqpNetLite](https://github.com/Azure/amqpnetlite) as the underlying AMQP 1.0 transport Protocol engine. 

### Overall Architecture
Apache-NMS-AMQP should bridge the familiar NMS concepts to AMQP protocol concepts as described in the document [amqp-bindmap-jms-v1.0-wd09.pdf](https://www.oasis-open.org/committees/download.php/60574/amqp-bindmap-jms-v1.0-wd09.pdf).
So in general most of the top level classes that implement the Apache.NMS interface _Connection, Session, MessageProducer,_ etc  create, manage, and destroy the amqpnetlite equivalent object _Connection, Session, Link,_ etc.

### Building With Visual Studio 2019
There are multiple projects: Apache-NMS-AMQP, Apache-NMS-AMQP.Test, and HelloWorld. All projects use the new csproj format available in Visual Studio 2019.
Apache-NMS-AMQP is the library which implements The Apache.NMS Interface using AmqpNetLite.
Apache-NMS-AMQP.Test produces an NUnit dll for unit testing.
HelloWorld is a sample application using the NMS library which can send messages to an AMQP Message Broker.

To build, launch Visual Studio 2019 with the nms-amqp.sln file and build the solution.
Build artifacts will be under `<root_folder>\<project_folder>\bin\$(Configuration)\$(TargetFramework)`.

### Building With DotNet SDK
Alternatively, to build without Visual Studio 2019 the project can be built using [.NET Core sdk tool](https://www.microsoft.com/net/download/windows), version 2.2.+.
Execute the dotnet sdk command to build all projects:
```
<root_folder>>dotnet build nms-amqp.sln 
```

### Testing
Tests use the NUnit Framework. The tests include both unit and system tests (require a broker). 

Apache-NMS-AMQP-Test contains only unit tests and doesn't require any configuration and dependencies. 

Apache-NMS-AMQP-Interop-Test contains system tests and require broker to be up and running. Broker can be configured either directly from the code (to do so you have to edit _AmqpTestSupport_ base class), or using environment variables:

| Variable | Meaning |
|----------|---------|
|NMS_AMQP_TEST_URI|Broker uri|
|NMS_AMQP_TEST_CU|Username|
|NMS_AMQP_TEST_CPWD|Password|

```
NUNIT3-CONSOLE Apache-NMS-AMQP-Interop-Test.dll -p:uri=brokerUri -p:cu=userName -p:cpwd=password
```

#### VS2019 Test Explorer
Visual Studio 2019 will also run NUnit tests with the built-in TestExplorer tool.

#### dotnet test 

If building with the dotnet sdk,  From the top level directory simply enter _dotnet test_ to build and run all the tests.  Individual tests can be run with:
```
dotnet test filter=<Test Name>
```

### Amqp Provider NMS Feature Support

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
| ISession | Y | 
| IQueue | Y | |
| ITopic | Y | |
| ITemporaryQueue | Y | |
| ITemporaryTopic | Y | |
| IMessageProducer | Y * | Anonymous producers are only supported on connections with the ANONYMOUS-RELAY capability. |
| MsgDeliveryMode.Persistent | Y | Producers will block on send until an outcome is received or will timeout after waiting the RequestTimeout timespan amount. Exceptions may be throw depending on the outcome or if the producer times out. |
| MsgDeliveryMode.NonPersistent | Y | Producers will not block on send nor expect to receive an outcome. Should an exception be raised from the outcome the exception will be delivered using the the connection ExceptionListener. |
| IMessageConsumer | Y * | Message Selectors and noLocal filter are not supported. |
| Durable Consumers | Y | |
| IQueueBrowser | N | The provider will throw NotImplementedException for the ISession create methods. |
| Configurable NMSMessageID and amqp serializtion | N | For future consideration. The prodiver will generate a MessageID from a sequence and serialize it as a string. |
| Flow control configuration | N | For future consideration. The provider will use amqpnetlite defaults except for initial link credits which is 200. |
| Object Deserialization Policy | N | For future consideration. The provider considers all Dotnet serialized objects in Object Message bodies are safe to deserialize. |
| Failover | Y

# Licensing 

This software is licensed under the terms you may find in the file named "LICENSE" in this directory.
