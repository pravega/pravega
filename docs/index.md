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

## Releases

The latest pravega releases can be found on the [Github Release](https://github.com/pravega/pravega/releases) project page.

## Quick Start

Read [Getting Started](getting-started.md) page for more information, and also visit [sample-apps](https://github.com/pravega/pravega-samples) repo for more applications. 

## Running Pravega

Pravega can be installed locally or in a distributed environment. The installation and deployment of pravega is covered in the [Running Pravega](deployment/deployment.md) guide.

## Support

Don’t hesitate to ask! Contact the developers and community on the mailing lists
if you need any help. Open an issue if you found a bug on [Github
Issues](https://github.com/pravega/pravega/issues)

## Documentation

The Pravega documentation of is hosted on the website:
<http://pravega.io/docs> or in the
[docs/](https://github.com/pravega/pravega/tree/master/docs) directory of the
source code.

## Contributing

Become one of the contributors! We thrive to build a welcoming and open
community for anyone who wants to use the system or contribute to it.
[Here](contributing.md) we describe how to
contribute to Pravega!

## About

Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on
GitHub.
