# NMS-AMQP
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
NMS-AMQP uses [AmqpNetLite](https://github.com/Azure/amqpnetlite) as the underlying AMQP 1.0 transport Protocol engine. 

### Overall Architecture
NMS-AMQP should bridge the familiar NMS concepts to AMQP protocol concepts as described in the document [amqp-bindmap-jms-v1.0-wd07.pdf](https://www.oasis-open.org/committees/download.php/59981/amqp-bindmap-jms-v1.0-wd07.pdf).
So in general most of the top level classes that implement the Apache.NMS interface _Connection, Session, MessageProducer,_ etc  create, manage, and destroy the amqpnetlite equivalent object _Connection, Session, Link,_ etc.

### Building
There are multiple projects: NMS-AMQP, NMS-AMQP.Test, and HelloWorld. All projects use the new csproj format available in Visual Studio 2017.
NMS-AMQP is the library which implements The Apache.NMS Interface using AmqpNetLite.
NMS-AMQP.Test produces an NUnit dll for unit testing.
HelloWorld is a sample application using the NMS library which can send messages to an AMQP Message Broker.

To build, launch Visual Studio 2017 with the nms-amqp.sln file and build the solution.
Build artifacts will be under `<root_folder>\<project_folder>\bin\$(Configuration)\$(TargetFramework)`.

Alternatively, to build without Visual Studio 2017 the project can be built using [.NET Core sdk tool](https://www.microsoft.com/net/download/windows), version 2.1.+.
Execute the dotnet sdk command to build all projects :
```
C:\<root_folder>>dotnet build nms-amqp.sln 
```
Note The .NetFramework target must be install for the dotnet sdk tool build the project with the tool.

### Testing
Tests use the NUnit Framework.  The tests include both unit and system tests (require a broker).  The test configuration is read from 'TestSuite.config' in the current directory.  For convenience the build copies 'test/TestSuite.config' to the 'bin' directory.

The NMS-AMQP.Test project builds a NUnit 3.7.0 unit test dll to be used by NUnit console, 3.7.0 or 3.8.0.
To run the unit tests use the nunit3-console.exe under the `<nuget_packages_location>\nunit.consolerunner\3.7.0\tools\` folder. 
**Eg:**
```
C:\Users\me\nms-amqp\test\bin\Debug\net452> nunit3-console.exe NMS.AMQP.Test.dll
```

#### VS2017 Test Explorer
Visual Studio 2017 will also run nunit tests with the built-in TestExplorer tool. If you wish to run tests inside VS2017 (useful for debugging) then you must configure _Test->Test Settings->Select Test Settings File_.  The *Test Settings File* is _test/config/Adapter.runsettings_.  

The file _Adapter.runsettings_ must be modified to reflect your local directory 
structure.

### Amqp Provider NMS Feature Support

| Feature       | Supported | Comments         |
|---------------|:---------:|:-----------------|
| TLS/SSL       | Y | Configuration is supported using transport properties. |
| Client Certificate Authentication | Y | Configuration is supported using transport properties. |
| Transactions (AcknowledgementMode.Transactional) | N | Session will throw NotSupportedException should a session's AcknowledgeMode be set to Transactional. |
| Distributed Transactions (INetTxConnectionFactory, INetTxConnection, INetTxSession) | N | |
| AcknowledgementMode.AutoAcknowledge | Y | |
| AcknowledgementMode.DupsOkAcknowledge | Y | |
| AcknowledgementMode.ClientAcknowledge | Y | |
| AcknowledgementMode.IndividualAcknowledge | Y | |
| IObjectMessage | Y * | Amqp value object bodies and dotnet serializable object bodies are supported. Java serialized objects can not be read by by the provider and are not supported. |
| IBytesMessage | Y | |
| IStreamMessage | Y | |
| IMapMessage | Y | |
| ITextMessage | Y | |
| IMessage | Y | |
| IConnectionFactory | Y | |
| IConnection | Y | The ConnectionInterruptedListener event and the ConnectionResumedListener are not supported. |
| ProducerTransformerDelegate | N | Any member access should throw a NotSupportedException. |
| ConsumerTransformerDelegate | N | Any member access should throw a NotSupportedException. |
| ISession | Y | Note all methods and events related to AcknowledgementMode.Transactional are not supported and should throw a NotSupportedException. |
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

# Licensing 

This software is licensed under the terms you may find in the file named "LICENSE" in this directory.
