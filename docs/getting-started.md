# Getting started


The best way to get to know Pravega is to start it up and run a sample Pravega
application.

## Running Pravega is simple


**Verify the following prerequisite**:

```
Java 8
```

**Download Pravega**

Download the pravega release from the [github releases page](https://github.com/pravega/pravega/releases).
If you prefer to build pravega yourself, you can download the code and run `./gradlew distribution`.

```
$ tar xfvz pravega-0.1.0.tgz
```

**Run Pravega in standalone mode**

This launches all the components of Pravega on your local machine.
NOTE: this is for testing/demo purposes only, *do not* use this mode of deployment 
in Production! More options for [Running Pravega](deployment/deployment.md) are
covered in the running pravega guide.           

```
$ cd pravega 
$ bin/pravega-standalone
```

Thats it.  Pravega should be up and running very soon.

## Running a sample Pravega App is simple too

Pravega maintains a separate github repository for sample applications.  It is located at:
[https://github.com/pravega/pravega-samples](https://github.com/pravega/pravega-samples)

Lets download and run the "Hello World" Pravega sample reader and writer apps.

**Generate the client jar files and publish them to the local maven repo** 
-   Note: you need to do this only if you have downloaded and are running a
        nightly build of Pravega.  If you are using a released build, the
        artifacts should already be in Maven Central.
-   Note: maven 2 needs to be installed and running on your machine

```
$ cd pravega 
$ ./gradlew publishMavenPublicationToMavenLocal
```
  -   The above command should generate the required jar files into your local
      maven repo.

**Download the Pravega-Samples git repo**

```
$ mkdir pravega-samples
$ cd pravega-samples
$ git clone https://github.com/pravega/pravega-samples
```

**Generate the scripts to run the apps**

```
$ cd pravega-samples/pravega-samples
$ ./gradlew installDist
```

**Run the sample "HelloWorldWriter"**
This runs a simple Java application that writes a "hello world" message
        as an event into a Pravega stream.
```
$ cd pravega-samples/pravega-samples/standalone-examples/build/install/pravega-standalone-examples
$ bin/helloWorldWriter
```
_Example HelloWorldWriter output_
```
...
******** Writing message: 'hello world' with routing-key: 'hello\_routingKey' to stream 'helloScope / helloStream'
...
```
See the [readme.md](https://github.com/pravega/pravega-samples/blob/master/standalone-examples/README.md) file in /HelloPravega for more details
    on running the HelloWorldWriter with different parameters

**Run the sample "HelloWorldReader"**

```
$ cd pravega-samples/pravega-samples/standalone-examples/build/install/pravega-standalone-examples
$ bin/helloWorldReader
```

_Example HelloWorldReader output_
```
...
******** Reading all the events from helloScope/helloStream
******** Read event 'hello world'
******** Read event 'null'
******** No more events from helloScope/helloStream
...
```

See the readme.md file in /HelloPravega for more details on running the
    HelloWorldReader application

**Congratulations!**  You have successfully installed Pravega and ran a couple of simple Pravega applications.
