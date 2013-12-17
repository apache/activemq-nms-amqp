=======================================================================
Welcome to:
 * Apache.NMS.AMQP : Apache NMS for AMQP Client Library
=======================================================================

For more information see http://activemq.apache.org/nms

=======================================================================
Building With NAnt 0.86 see http://nant.sourceforge.net/
=======================================================================

NAnt version 0.86 or newer is required to build Apache.NMS.AMQP.  Version 0.90
or newer is highly recommended.
To build the code using NAnt, run:

  nant

To run the unit tests you need to run an Apache ActiveMQ Broker first then run:

  nant test

The NMS documentation can be generated into three different formats using
Microsoft's Sandcastle open source product. The Sandcastle Styles project
was used to enhance the output generated from the current release of Sandcastle.

The Sandcastle project is located here:

http://sandcastle.codeplex.com/

The Sandcastle Styles project is located here:

http://sandcastlestyles.codeplex.com/

To generate the documentation, run:

  nant sandcastle-all

=======================================================================
Building With Visual Studio 2008 (net-2.0 only)
=======================================================================

First build the project with nant, this will download and install 
all the 3rd party dependencies for you.

Open the solution File.  Build using "Build"->"Build Solution" 
menu option.

The resulting DLLs will be in build\${framework}\debug or the 
build\${framework}\release directories depending on your settings 
under "Build"->"Configuration Manager"

If you have the Resharper plugin installed in Visual Studio, you can run 
all the Unit Tests by using the "ReSharper"->"Unit Testing"->"Run All 
Tests from Solution" menu option.  Please note that you must run an 
Apache ActiveMQ Broker before kicking off the unit tests.  Otherwise,
the standalone NUnit test runner can be used.  NUnit version 2.5.8
is required to build and run the unit tests.

=======================================================================
Building With Visual Studio 2010 (net-4.0 only)
=======================================================================

First build the project with nant, this will download and install 
all the 3rd party dependencies for you.

Open the solution File.  Build using "Build"->"Build Solution" 
menu option.

The resulting DLLs will be in build\${framework}\debug or the 
build\${framework}\release directories depending on your settings 
under "Build"->"Configuration Manager"


