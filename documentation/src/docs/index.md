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

-   Exactly-Once Semantics - Ensure that each event is delivered and processed exactly once, with exact ordering guarantees, despite failures in clients, servers or the network.

-   Auto Scaling - Unlike systems with static partitioning, Pravega can automatically scale individual data streams to accommodate changes in data ingestion rate.

-   Distributed Computing Primitive - Pravega is great for distributed computing; it can be used as a data storage mechanism, for messaging between processes and for other distributed computing services such as leader election.

-   Write Efficiency - Pravega shrinks write latency to milliseconds, and seamlessly scales to handle high throughput reads and writes from thousands of concurrent clients, making it ideal for IoT and other time sensitive applications.

-   Unlimited Retention - Ingest, process and retain data in streams forever.It uses the same paradigm to access both real-time and historical events stored in Pravega.

-   Storage Efficiency - Pravega can be used to build pipelines of data processing, combining batch, real-time and other applications without duplicating data for every step of the pipeline.

-   Durability - In Pravega, there is no compromise between performance, durability and consistency.
    Pravega persists and protects data before the write operation is acknowledged to the client.
    
-   Transaction Support - Pravega Transactions can be used by the developer to ensure that a set of events are written to a stream atomically.

-   Security and pluggable role based access control - Pravega can be deployed by the administrators securely by enabling TLS for communications and can deploy their own implementation of role based access control plugin.   


## Releases

The latest pravega releases can be found at [Github Release](https://github.com/pravega/pravega/releases).

## Quick Start

Read [Getting Started](getting-started.md) page for more information, and also visit [sample-apps](https://github.com/pravega/pravega-samples) repo for more applications. 

## Frequently Asked Questions

 List of frequently asked questions can be found at [here](faq.md).

## Running Pravega

Pravega can be installed locally or in a distributed environment. The installation and deployment of Pravega is covered in the [Running Pravega](deployment/deployment.md) guide.

## Pravega Security, Role based access control and TLS
Pravega supports encryption of all communication channels and pluggable role based access control.
Find more details here:
1. [TLS](security/pravega-security-encryption.md)
2. [Authorization, authentication and RBAC](security/pravega-security-authorization-authentication.md)

## Support

Don’t hesitate to ask! Contact the developers and community on the [mailing lists](https://groups.google.com/forum/#!forum/pravega-users) or on [slack](https://pravega-io.slack.com/) if you need any help. 
Open an issue if you found a bug on [Github
Issues](https://github.com/pravega/pravega/issues).

## Contributing

Become one of the contributors! We thrive to build a welcoming and open
community for anyone who wants to use the system or contribute to it.
[Here](contributing.md) we describe how to
contribute to Pravega! You can see the roadmap document [here](roadmap.md).

## About

Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on
GitHub.
