# Pravega Overview

Pravega is an open source distributed storage service implementing **Streams**. It offers Stream as the main primitive for the foundation of reliable storage systems: a *high-performance, durable, elastic, and infinite append-only byte stream with strict ordering and consistency*.

### Features 

-   Auto Scaling - Dynamically scale data streams across storage
    and processing resources to accommodate ever changing data ingestion
    rate.

-   Infinite Retention - Automatically migrate old events in a stream to low-cost cloud-scale
    storage system, thus using the same access paradigm for both real-time and historical analysis.

-   Durability - Don't compromise between performance, durability and consistency.
    Pravega replicates and persists the ingested event before acknowledging while 
    maintaining low latency and strong consistency.
    
-   Exactly-Once Semantics - Ensure that each message is delivered and processed
    exactly once despite failures with retransmissions, idempotence and transactions.
    
-   Transaction Support - Use transactional data ingestion to guarantee that a set
    of events are added to a stream atomically.

-   Multi-Protocol Access - With multi-protocol capability, Access and process
    your streams not only via Pravega Streaming API, but via HDFS protocol.

-   Write Efficiency - Pravega shrinks to milliseconds the time it takes to write 
    massive volumes durably by seamlessly scaling up to handle high throughput 
    reads and writes from thousands of concurrent clients.

-   Fault Tolerant - Pravega detects failures and
    automatically recovers ensuring the continuous flow of data required for 
    business continuity. 

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

There are multiple ways to deploy your own Pravega Cluster. These  installation options currently include [manual](http://pravega.io/docs/Manual-Deployment-Pravega-Cluster/) and [docker based](http://pravega.io/docs/Docker-based-Deployment-Pravega-Cluster/). As we are enabling more options with help of the community, check out [Deploying Pravega Cluster](http://pravega.io/docs/Deploying-Pravega/) page for more. 


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
