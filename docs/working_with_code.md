# Prerequisites
- Visual Studio 2019. [Community Edition](https://visualstudio.microsoft.com/downloads/) works.

# Build the projects
- Build with Visual Studio. Open apache-nms-amqp in Visual Studio. This solution contains both source code and test projects for all supported platforms. If the SDK of a particular platform is not present, the project will fail to load. You can either install the required SDK (if available) or remove the project(s) from solution.

- Alternatively, to build without Visual Studio the solution can be built using [.NET Core SDK tool](https://www.microsoft.com/net/download/windows), version 2.2.+.

Execute the dotnet sdk command to build all projects:
```
<root_folder>>dotnet build nms-amqp.sln 
```

# Run the tests
Tests use the NUnit Framework. The tests include both unit and system tests (require a broker). 

Apache-NMS-AMQP-Test contains only unit tests and doesn't require any configuration and dependencies. 

Apache-NMS-AMQP-Interop-Test contains system tests and require broker to be up and running. Broker can be configured either directly from the code (to do so you have to edit _AmqpTestSupport_ base class), or using environment variables:

| Variable | Meaning |
|----------|---------|
|NMS_AMQP_TEST_URI|Broker uri|
|NMS_AMQP_TEST_CU|Username|
|NMS_AMQP_TEST_CPWD|Password|

## Visual Studio Test Explorer
Visual Studio 2019 will run NUnit tests with the built-in TestExplorer tool.

## dotnet test 

If building with the dotnet sdk, from the top level directory simply enter _dotnet test_ to build and run all the tests. Individual tests can be run with:

```
dotnet test filter=<Test Name>
```