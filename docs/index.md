# Pravega Overview

Pravega is an open source storage primitive implementing **Streams** for continuous and unbounded data. A Pravega stream is a durable, elastic, append-only, unbounded sequence of bytes that has good performance and strong consistency.  

Read [Pravega Concepts](pravega-concepts.md) for more details.

## Key Features 

-   Exactly-Once Semantics - Ensure that each event is delivered and processed exactly once, with exact ordering guarantees, despite failures in clients, servers or the network.

-   Auto Scaling - Unlike systems with static partitioning, Pravega can automatically scale individual data streams to accommodate changes in data ingestion rate.

-   Distributed Computing Primitive - Pravega is great for distributed computing; it can be used as a data storage mechanism, for messaging between processes and for other distributed computing services such as leader election.

-   Write Efficiency - Pravega shrinks write latency to milliseconds, and seamlessly scales to handle high throughput reads and writes from thousands of concurrent clients, making it ideal for IoT and other time sensitive applications.

-   Infinite Retention - Ingest, process and retain data in streams forever. Use same paradigm to access both real-time and historical events stored in Pravega.

-   Storage Efficiency - Use Pravega to build pipelines of data processing, combining batch, real-time and other applications without duplicating data for every step of the pipeline.

-   Durability - Don't compromise between performance, durability and consistency.
    Pravega persists and protects data before the write operation is acknowledged to the client.
    
-   Transaction Support - A developer uses a Pravega Transaction to ensure that a set of events are written to a stream atomically.


## Releases

The latest pravega releases can be found on the [Github Release](https://github.com/pravega/pravega/releases) project page.

## Quick Start

Read [Getting Started](getting-started.md) page for more information, and also visit [sample-apps](https://github.com/pravega/pravega-samples) repo for more applications. 

## Documentation

The Pravega documentation is hosted on the website:
<http://pravega.io/docs> or in the
[docs/](https://github.com/pravega/pravega/tree/master/docs) directory of the
source code.

## Running Pravega

Pravega can be installed locally or in a distributed environment. The installation and deployment of Pravega is covered in the [Running Pravega](deployment/deployment.md) guide.

## Support

Don’t hesitate to ask! Contact the developers and community on the mailing lists
if you need any help. Open an issue if you found a bug on [Github
Issues](https://github.com/pravega/pravega/issues)

## Contributing

Become one of the contributors! We thrive to build a welcoming and open
community for anyone who wants to use the system or contribute to it.
[Here](contributing.md) we describe how to
contribute to Pravega!

## About

Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on
GitHub.
