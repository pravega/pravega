<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Getting Started


The best way to get to know Pravega is to start it up and run a sample Pravega
application.

## Running Pravega is Simple


**Verify the following prerequisite**

```
Java 8
```

**Download Pravega**

Download the Pravega release from the [Github Releases](https://github.com/pravega/pravega/releases).
If you prefer to build Pravega yourself, you can download the code and run `./gradlew distribution`. More
details are shown in the Pravega [README](https://github.com/pravega/pravega/blob/master/README.md).

```
$ tar xfvz pravega-<version>.tgz
```

**Run Pravega in standalone mode**

This launches all the components of Pravega on your local machine.

**Note:** This is for testing/demo purposes only, **do not** use this mode of deployment
in Production!

More options and additional ways to run Pravega can be found in [Running Pravega](deployment/deployment.md) guide.

```
$ cd pravega-<version>
$ bin/pravega-standalone
```

The command above runs Pravega locally for development and testing purposes. It does not persist in the storage tiers like we do with a real deployment of Pravega and as such you shouldn't expect it to recover from crashes, and further, not rely on it for production use. For production use, we strongly encourage a full deployment of Pravega.

## Running a sample Pravega Application

We have developed a few samples to introduce the developer to coding with Pravega here: [Pravega Samples](https://github.com/pravega/pravega-samples).

Download and run the "Hello World" Pravega sample reader and writer applications. Pravega
dependencies will be pulled from maven central.

**Note:** The samples can also use a locally compiled version of Pravega. For more information,
please see the [README](../../../README.md) note on maven publishing.

**Download the Pravega-Samples git repo**

```
$ git clone https://github.com/pravega/pravega-samples
$ cd pravega-samples
```

**Generate the scripts to run the applications**

```
$ ./gradlew installDist
```

**Run the sample "HelloWorldWriter"**

This runs a simple Java application that writes a "hello world" message
        as an event into a Pravega stream.
```
$ cd pravega-samples/pravega-client-examples/build/install/pravega-client-examples
$ bin/helloWorldWriter
```
_Example HelloWorldWriter output_
```
...
Writing message: 'hello world' with routing-key: 'helloRoutingKey' to stream 'examples / helloStream'
...
```
See the [README](https://github.com/pravega/pravega-samples/blob/v0.4.0/pravega-client-examples/README.md) file in the standalone-examples for more details
    on running the HelloWorldWriter with different parameters.

**Run the sample "HelloWorldReader"**

```
$ cd pravega-samples/pravega-client-examples/build/install/pravega-client-examples
$ bin/helloWorldReader
```

_Example HelloWorldReader output_
```
...
Reading all the events from examples/helloStream
...
Read event 'hello world'
No more events from examples/helloStream
...
```

See the [README](https://github.com/pravega/pravega-samples/blob/v0.4.0/pravega-client-examples/README.md) file in the pravega-client-examples for more details on running the
    HelloWorldReader application.
