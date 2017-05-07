# Getting started


The best way to get to know Pravega is to start it up and run a sample Pravega
application.

## Running Pravega is simple


**Verify the following prerequisite**:

```
Java 8
```

**Download Pravega**

Download the Pravega release from the [github releases page](https://github.com/pravega/pravega/releases).
If you prefer to build Pravega yourself, you can download the code and run `./gradlew distribution`. More 
details are shown in the Pravega [README.md](https://github.com/pravega/pravega/blob/master/README.md).

```
$ tar xfvz pravega-0.1.0.tgz
```

**Run Pravega in standalone mode**

This launches all the components of Pravega on your local machine.
NOTE: this is for testing/demo purposes only, *do not* use this mode of deployment 
in Production! More options for [Running Pravega](deployment/deployment.md) are
covered in the running Pravega guide.           

```
$ cd pravega-0.1.0
$ bin/pravega-standalone
```

That's it.  Pravega should be up and running very soon.

You can find additional ways to run Pravega in [Running Pravega](deployment/deployment.md).

## Running a sample Pravega App is simple too

Pravega maintains a separate github repository for sample applications.  It is located at:
[https://github.com/pravega/pravega-samples](https://github.com/pravega/pravega-samples)

Lets download and run the "Hello World" Pravega sample reader and writer apps. Pravega
dependencies will be pulled from maven central.

Note: The samples can also use a locally compiled version of Pravega. For more information
about this see the note on maven publishing in the [README.md](https://github.com/pravega/pravega/blob/master/README.md).

**Download the Pravega-Samples git repo**

```
$ git clone https://github.com/pravega/pravega-samples
$ cd pravega-samples
```

**Generate the scripts to run the apps**

```
$ ./gradlew installDist
```

**Run the sample "HelloWorldWriter"**
This runs a simple Java application that writes a "hello world" message
        as an event into a Pravega stream.
```
$ cd pravega-samples/standalone-examples/build/install/pravega-standalone-examples
$ bin/helloWorldWriter
```
_Example HelloWorldWriter output_
```
...
Writing message: 'hello world' with routing-key: 'helloRoutingKey' to stream 'examples / helloStream'
...
```
See the [readme.md](https://github.com/pravega/pravega-samples/blob/master/standalone-examples/README.md) file in the standalone-examples for more details
    on running the HelloWorldWriter with different parameters

**Run the sample "HelloWorldReader"**

```
$ cd pravega-samples/standalone-examples/build/install/pravega-standalone-examples
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

See the readme.md file in the standalone-examples for more details on running the
    HelloWorldReader application

**Congratulations!**  You have successfully installed Pravega and ran a couple of simple Pravega applications.
