<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Overview

Pravega is an open source storage primitive implementing **Streams** for continuous and unbounded data. A Pravega stream is a durable, elastic, append-only, unbounded sequence of bytes that has good performance and strong consistency.  

Read [Pravega Concepts](pravega-concepts.md) for more details.

## Key Features 

-   Exactly-Once Semantics:- Pravega ensures that each event is delivered and processed exactly once, with exact ordering guarantees, despite failures in clients, servers or the network.

-   Auto Scaling:- Pravega performs auto scaling by automatically scaling the individual data streams to accommodate changes in data ingestion rate.

-   Distributed Computing Primitive:- Pravega is very efficient in performing distributed computing. It can be used as a data storage mechanism. It can be used for messaging between processes and for other distributed computing services such as leader election.

-   Write Efficiency:- Pravega shrinks write latency to milliseconds, and seamlessly scales to handle high throughput reads and writes from thousands of concurrent clients, making it ideal for IoT and other time sensitive applications.

-   Unlimited Retention:- Pravega performs ingestion, processing and retains data in streams forever. Pravega uses the same paradigm to access both real-time and historical events stored in it.

-   Storage Efficiency:-  Pravega is used to build pipelines of data processing. It is used to combine batch, real-time and other applications without duplicating data for every step of the pipeline.

-   Durability:- Pravega do not compromise between performance, durability and consistency. Pravega persists and protects data before the write operation is acknowledged to the client.
    
-   Transaction Support:-  Pravega Transaction is used by the developer to ensure that a set of events are written to a stream atomically.


## Releases

The latest pravega releases can be found on the [Github Release](https://github.com/pravega/pravega/releases) project page.

## Quick Start

Read [Getting Started](getting-started.md) page for more information, and also visit [sample-apps](https://github.com/pravega/pravega-samples) repo for more applications. 

## Frequently Asked Questions

You can find a list of frequently asked questions [here](faq.md).

## Running Pravega

Pravega can be installed locally or in a distributed environment. The installation and deployment of Pravega is covered in the [Running Pravega](deployment/deployment.md) guide.

## Support

Don’t hesitate to ask! Contact the developers and community on the [mailing lists](https://groups.google.com/forum/#!forum/pravega-users) or on [slack](https://pravega-io.slack.com/) if you need any help. 
Please open an issue in [Github
Issues](https://github.com/pravega/pravega/issues) if you found a bug.

## Contributing

Become one of the contributors! We thrive to build a welcoming and open
community for anyone who wants to use the system or contribute to it.
We describe how to contribute to Pravega [here](contributing.md)! You can see the roadmap document [here](roadmap.md).

## About

Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on
GitHub.
