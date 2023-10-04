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
# Pravega Overview

Pravega is a storage system that exposes **Stream** as the main primitive for continuous and unbounded data. A Pravega stream is a durable, elastic, append-only, unbounded sequence of bytes having good performance and strong consistency.  

Read [Pravega Concepts](pravega-concepts.md) for more details.

## Key Features 

-   **Exactly-Once Semantics:** Pravega ensures that each event is delivered and processed exactly once, with exact ordering guarantees, despite failures in clients, servers or the network.

-   **Auto Scaling:** Pravega automatically changes the parallelism of the individual data streams to accommodate fluctuations in data ingestion rate.

-   **Distributed Computing Primitive:** Pravega is not only a great storage service for data streams, it can also be used as a durable and consistent messaging service across processes. Pravega provides unique abstractions, which aids the users to build advanced services, like distributed consensus and leader election.

-   **Write Performance:** Pravega shrinks write latency to milliseconds, and seamlessly scales to handle high throughput reads and writes from thousands of concurrent clients, making it ideal for IoT and other time sensitive applications.

-   **Unlimited Retention:** Pravega decouples brokering of events from the actual data storage. This allows Pravega to transparently move data events from low-latency, durable storage tier to a cloud storage service (e.g., HDFS, Amazon S3, or DellEMC Isilon/ECS), while clients are agnostic to the actual location of data.

-   **Storage Efficiency:** Pravega is used to build data processing pipelines that may combine batch and real-time applications without duplicating data for every step of the pipeline. This is possible because Pravega unifies stream (ordered) and batch (parallel) access to data events for data processing engines.

-   **Durability:** Pravega persists and protects data events once the write operation is acknowledged to the client.

-   **Transaction Support:** A Pravega Transaction ensures that a set of events are written to a stream atomically. This is a key feature for distributed streaming applications requiring exactly-once guarantees on their output.

-   **Security and pluggable role-based access control:** Pravega can be deployed by the administrators securely by enabling TLS for communications and can deploy their own implementation of role-based access control plugin.   

## Quick Start

Read [Getting Started](getting-started.md) page for more information, and also visit [Pravega samples](https://github.com/pravega/pravega-samples) repository for more applications.

## Running Pravega

Pravega can be installed locally or in a distributed environment. The installation and deployment of Pravega is covered in the [Running Pravega](deployment/deployment.md) guide.

## Releases

The latest Pravega releases can be found on the [Github Release](https://github.com/pravega/pravega/releases) project page.

## Support

Don’t hesitate to ask! Contact the developers and community on the [Slack](https://pravega-io.slack.com/) or email at security@pravega.io if you need any help.
Please open an issue in [Github Issues](https://github.com/pravega/pravega/issues) if you find a bug.

## Contributing

Become one of the **contributors!** We thrive to build a welcoming and open
community for anyone who wants to use the system or contribute to it.
Please check the [Contributions Guidelines](contributing.md) to quickly understand on how to contribute to Pravega? You can see the [Roadmap](roadmap.md) document for more information.

## About

Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on
GitHub.
