# Pravega [![Build Status](https://travis-ci.com/pravega/pravega.svg?token=qhH3WLZqyhzViixpn6ZT&branch=master)](https://travis-ci.com/pravega/pravega) [![codecov](https://codecov.io/gh/pravega/pravega/branch/master/graph/badge.svg?token=6xOvaR0sIa)](https://codecov.io/gh/pravega/pravega)

Pravega is an open source distributed storage service implementing **Streams**. It offers Stream as the main primitive for the foundation of reliable storage systems: a *high-performance, durable, elastic, and infinite append-only byte stream with strict ordering and consistency*.

To learn more about Pravega, visit http://pravega.io

### Features 

-   Auto Scaling - Unlike systems with static partitioning, Pravega can automatically
    scale individual data streams to accommodate changes in data ingestion rate.

-   Infinite Retention - Unlike systems with static partitioning, Pravega can 
    automatically scale individual data streams to accommodate changes in data
    ingestion rate.

-   Durability - Don't comprise between performance, durability and consistency. 
    Pravega persists and protects data before the write operation is acknowledged 
    to the client.
    
-   Exactly-Once Semantics - Despite failures, ensure that each event is delivered and 
    processed exactly once with retransmissions, idempotence and transactions. 
    
-   Transaction Support - A developer uses a Pravega Transaction to ensure that 
    a set of events are written to a stream atomically.

-   Multi-Protocol Access - Use Pravega to build pipelines of data processing, 
    combining batch, real-time and other applications without duplicating data 
    for every step of the pipeline.

-   Write Efficiency - Pravega shrinks write latency to milliseconds, and seamlessly 
    scales to handle high throughput reads and writes from thousands of concurrent 
    clients, making it ideal for IoT and other time sensitive applications.

-   Fault Tolerant - Pravega detects failures and automatically recovers ensuring 
    data availability.    

### Create stream

```
StreamManager streamManager = StreamManager.create(controllerURI);

// Auto scaling is disabled. Stream has fixed number of segments. 
// Scaling policy can be fixed, byEventRate. or byDataRate

StreamConfiguration streamConfig = StreamConfiguration.builder()
        .scalingPolicy(ScalingPolicy.fixed(5))
        .build();
        
final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

```

### Write events to a steam 

```
ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
           new JavaSerializer<String>(),
           EventWriterConfig.builder().build());

final AckFuture writeFuture = writer.writeEvent(routingKey, message);
```

### Read events from a stream 

```
EventStreamReader<String> reader = clientFactory.createReader("Reader1",
        'MyReaderGroup1',
        new JavaSerializer<String>(),
        ReaderConfig.builder().build());
        
EventRead<String> event = reader.readNextEvent(READER_TIMEOUT_MS);

```

Quick Start
----------------------------

Read [Getting Started](http://pravega.io/docs/Getting-Started/) page for more information, and also visit [sample-apps](https://github.com/pravega/pravega-samples) repo for more applications. 


Deployment Options 
-------------------

There are multiple ways to deploy your own Pravega Cluster. These  installation options currently include [manual](http://pravega.io/docs/deployment-manual/) and [docker based](http://pravega.io/docs/deployment-docker/). As we are enabling more options with help of the community, check out [Deploying Pravega Cluster](http://pravega.io/docs/deployment/) page for more. 


Support
-------

Don’t hesitate to ask! Contact the developers and community on the mailing lists
if you need any help. Open an issue if you found a bug on [Github
Issues](https://github.com/pravega/pravega/issues)

Documentation
-------------

The Pravega documentation of is hosted on the website:
<http://pravega.io/docs> or in the
[docs/](https://github.com/pravega/pravega/tree/master/docs) directory of the
source code.

Contribute
----------

Become one of the contributors! We thrive to build a welcoming and open
community for anyone who wants to use the system or contribute to it.
[Here](https://github.com/pravega/pravega/wiki/Contributing) we describe how to
contribute to Pravega!

About
-----

Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on
GitHub.
