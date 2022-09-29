<!--
Copyright Pravega Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Pravega - Creating Your First Application

Learn how to create a Hello World Pravega app. This guide covers:

* Starting Pravega standalone.
  
* Creating a Pravega Stream.
  
* Creating an Event Writer and write events into a Pravega Stream.
  
* Create an Event Reader and read events from the Pravega Stream.


# 1. Prerequisites
To complete this guide, you need:

* Less than 15 minutes
  
* An IDE
  
* JDK 11+ installed with JAVA_HOME configured appropriately
  
* <details>
  <summary>Gradle 6.5.1+</summary>
  Installation : https://gradle.org/install/
  
  Note: Verify Gradle is using the Java you expect. You can verify which JDK Gradle uses by running `gradle --version`.
</details>

# 2. Goal
In this guide, we will develop a straightforward application that creates a Stream on Pravega and writes an event into the Stream and reads back from it.
We recommend that you follow the instructions from [Bootstrapping project](#4-Bootstrapping-the-Project) onwards to create the application step by step.
However, you can go straight to the completed example at [Pravega-samples-repo](https://github.com/pravega/pravega-samples).

<details open>
<summary>Command to clone the pravega-samples repo:</summary>
<p>

```console
git clone https://github.com/pravega/pravega-samples.git
```

</p>
</details>  


The solution is located in the pravega-client-examples [directory]( https://github.com/pravega/pravega-samples/tree/master/pravega-client-examples/src/main/java/io/pravega/example/gettingstarted ).

# 3. Starting Pravega Standalone
In standalone mode, the Pravega server is accessible from clients through the `localhost` interface only. Controller REST APIs, however, are accessible from remote hosts/machines.
You can launch a standalone mode server using either of the following options:

1. From source code:

<details open>
<summary>Commands to start standalone from source code:</summary>
<p>

Checkout the source code.
```console
git clone https://github.com/pravega/pravega.git
cd pravega
```

Build the Pravega standalone mode distribution.
```console
./gradlew startStandalone
```

</p>
</details>

2. From installation package:

<details open>
<summary>Commands to start standalone from installation package:</summary>
<p>

Download the Pravega release from the [GitHub Releases](https://github.com/pravega/pravega/releases).

```console
tar xfvz pravega-<version>.tgz
```
Download and extract either tarball or zip files. Follow the instructions provided for the tar files (same can be applied for zip file) to launch all the components of Pravega on your local machine.

Run Pravega Standalone:

```console
pravega-<version>/bin/pravega-standalone
```

</p>
</details>  

**Note:** In order to remotely troubleshoot Java application, JDWP port is being used. The default JDWP port in Pravega(8050) can be overridden using the below command before starting Pravega in standalone mode. 0 to 65535 are allowed range of port numbers.

```
$ export JDWP_PORT=8888
```

# 4. Bootstrapping the Project

The easiest way to bootstrap a sample application against Pravega is to run the following command in a folder of your choice.
```console
gradle init --type java-application
```
Add the below snippet to dependencies section of build.gradle in the app directory.
```groovy
// https://mvnrepository.com/artifact/io.pravega/pravega-client
implementation group: 'io.pravega', name: 'pravega-client', version: '0.9.0'
```
Invoke `gradle run` to run the project.


<details open>
<summary>Expected output:</summary>
<p>

```console
gradle run

> Task :app:run
Hello World!

BUILD SUCCESSFUL in 890ms
2 actionable tasks: 2 executed
```

</p>
</details>

## 4.1 Create a Pravega Stream

Let's first get to know Pravega's client APIs by creating a stream with a fixed scaling policy of 1 segment. We'll need a StreamConfiguration to define this.
```java
StreamConfiguration streamConfig = StreamConfiguration.builder()
        .scalingPolicy(ScalingPolicy.fixed(1))
        .build();
```
With this `streamConfig`, we can create streams that feel a bit more like traditional append-only files. Streams exist within Scopes, which provide a namespace for related Streams. We use a [StreamManager](https://pravega.io/docs/latest/javadoc/clients/io/pravega/client/admin/StreamManager.html) to tell a Pravega cluster controller to create our scope and our streams. Since we are using a standalone Pravega let's use the below controller address.
```java
URI controllerURI = URI.create("tcp://localhost:9090");
```
The code to create a Pravega Stream is as follows.
```java
try (StreamManager streamManager = StreamManager.create(controllerURI)) {
        streamManager.createScope("examples");
        streamManager.createStream("examples", "helloStream", streamConfig);
}
```
Executing the above lines should ensure we have created a Pravega scope called `examples` and a Pravega Stream called `helloStream`.

## 4.2 Create a Pravega Event Writer and write events into the stream

Let's create a Pravega Event Writer using the [EventStreamClientFactory](https://pravega.io/docs/latest/javadoc/clients/io/pravega/client/EventStreamClientFactory.html).

```java
try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope("examples",
                ClientConfig.builder().controllerURI(controllerURI).build());
     EventStreamWriter<String> writer = clientFactory.createEventWriter("helloStream",
                new UTF8StringSerializer(), EventWriterConfig.builder().build())) {
        writer.writeEvent("helloRoutingKey", "hello world!"); // write an event.
}
```
The above snippet creates an Event Writer and writes an event into the Pravega stream. Note that `writeEvent()` returns a `CompletableFuture`, which can be captured for use or will be resolved when calling `flush()` or `close()`, and, if destined for the same segment, the futures write in the order `writeEvent()` is called.

When instantiating the EventStreamWriter above, we passed in a [UTF8StringSerializer](https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/stream/impl/UTF8StringSerializer.java) instance. Pravega uses a [Serializer](https://pravega.io/docs/latest/javadoc/clients/io/pravega/client/stream/Serializer.html) interface in its writers and readers to simplify the act of writing and reading an object's bytes to and from Streams. The [JavaSerializer](https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/stream/impl/JavaSerializer.java) can handle any `Serializable` object.

## 4.3 Create a Pravega Event Reader and read the event back from the stream

Readers are associated with Reader Groups, which track the Readers' progress and allow more than one Reader to coordinate over which segments they'll read.
A [ReaderGroupManager](https://pravega.io/docs/latest/javadoc/clients/io/pravega/client/admin/ReaderGroupManager.html) is used to create a new reader group on the Pravega Stream.


The below snippet creates a [ReaderGroup](https://pravega.io/docs/latest/javadoc/clients/io/pravega/client/stream/ReaderGroup.html).
```java
try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope("examples", controllerURI)) {
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of("examples", "helloStream"))
                .disableAutomaticCheckpoints()
                .build();
        readerGroupManager.createReaderGroup("readerGroup", readerGroupConfig);
}
```
We can attach a Pravega Event Reader to this Reader Group and read the data from the Pravega Stream `helloStream`. The below snippet creates an EventReader called `reader` and reads the value from the Pravega Stream.

```java
try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope("examples",
        ClientConfig.builder().controllerURI(controllerURI).build());
        EventStreamReader<String> reader = clientFactory.createReader("reader",
        "readerGroup",
        new UTF8StringSerializer(),
        ReaderConfig.builder().build())) {
        String event = reader.readNextEvent(5000).getEvent();
        System.out.println(event);
}
```

# 5. What's next?
This guide covered the creation of a application that writes and reads from Pravega. However, there is much more. We recommend continuing the journey by going through [Pravega-client-101](https://blog.pravega.io/2020/09/22/pravega-client-api-101/) and other samples present in the [Pravega Samples repo](https://github.com/pravega/pravega-samples).

