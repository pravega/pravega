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
# Frequently Asked Questions

### What is Pravega?
Pravega is an open source storage primitive implementing **Streams** for continuous and unbounded data. See [here](terminology.md) for more definitions of terms related to Pravega.

### What does "Pravega" mean?
"Pravega" is a word from Sanskrit referring to "good speed".

### Is Pravega similiar to systems such as Kafka and Kinesis?
Pravega is built from the ground up as an enterprise grade storage system to support features such as exactly once, durability etc. Pravega is an ideal store for streaming data, data from real-time applications and IoT data.

### How can I participate in open source?
Disruptive innovation is accelerated by open source. When Pravega was created, there was no question it made sense to make it open source.
We welcome contributions from experienced and new developers alike. Check out the code in [Github](https://github.com/pravega/pravega). More detail about how to get involved can be found [here](contributing.md).

### How do I get started?
Read the [Getting Started](getting-started.md) guide for more information, and also visit [sample-apps](https://github.com/pravega/pravega-samples) repo for some sample applications.

### I am stuck. Where can I get help?
Don’t hesitate to ask! Contact the developers and community on the mailing lists
if you need any help. See [Join the Community](join-community.md) for more details.

### Does Pravega support exactly once semantics?
Absolutely. See [Key Features](key-features.md) for a discussion on how Pravega supports exactly once semantics.

### How does Pravega work with stream processors such as Apache Flink?
So many features of Pravega make it ideal for stream processors. First, Pravega comes out of the box with a Flink connector. Critically, Pravega provides exactly once semantics, making it much easier to develop accurate stream processing applications. The combination of exactly once semantics, durable storage and transactions makes Pravega an ideal way to chain Flink jobs together, providing end-end consistency and exactly once semantics. See [here](key-features.md) for a list of key features of Pravega.

### How does auto scaling work between stream processors and Flink?
Auto scaling is a feature of Pravega where the number of segments in a stream changes based on the ingestion rate of data. If data arrives at a faster rate, Pravega increases the capacity of a stream by adding segments. When the data rate falls, Pravega can reduce capacity of a stream. As Pravega scales up and down the capacity of a stream, applications, such as a Flink job can observe this change and respond by adding or reducing the number of job instances consuming the stream. See the "Auto Scaling" section in [Key Features](key-features.md) for more discussion of auto scaling.

### What consistency guarantees does Pravega provide?
Pravega makes several guarantees. Durability - once data is acknowledged to a client, Pravega guarantees it is protected. Ordering - events with the same routing key will always be read in the order they were written. Exactly once - data written to Pravega will not be duplicated.

### Why is supporting consistency and durability so important for storage systems such as Pravega?
Primarily because it makes building applications easier. Consistency and durability are key for supporting exactly once semantics. Without exactly once semantics, it is difficult to build fault tolerant applications that consistency produce accurate results. See [Key Features](key-features.md) for a discussion on consistency and durability guarantees play a role in Pravega's support of exactly once semantics.

### Does Pravega support transactions?
Yes. The Pravega API allows an application to create a transaction on a stream and write data to the transaction. The data is durably stored, just like any other data written to Pravega. When the application chooses, it can commit or abort the transaction. When a transaction is committed, the data in the transaction is atomically appended to the stream. See [here](transactions.md) for more details on Pravega's transaction support.

### Does Pravega support transactions across different routing keys?
Yes. A transaction in Pravega is itself a stream; it can have 1 or more segments and data written to the transaction is placed into the segment associated with the data's routing key. When the transaction is committed, the transaction data is appended to the appropriate segment in the stream.

### Do I need HDFS installed in order to use Pravega?
Yes. Normally, you would deploy an HDFS for Pravega to use as its Tier 2 storage. However, for simple test/dev environments, the so-called standAlone version of Pravega provides its own simulated HDFS. See the [Running Pravega](deployment/deployment.md) guide for more details.

### Which Tier 2 storage systems does Pravega support?
Pravega is designed to support various types of Tier 2 storage systems. Currently we have implemented HDFS as the first embodiment of Tier 2 storage.

### What distributed computing primitives does Pravega provide?
Pravega provides an API construct called StateSynchronizer. Using the StateSynchronizer, a developer can use Pravega to build synchronized shared state between multiple processes. This primitive can be used to build all sorts of distributed computing solutions such as shared configuration, leader election, etc. See the "Distributed Computing Primitive" section in [Key Features](key-features.md) for more details.

### What hardware do you recommend for Pravega?
The Segment Store requires faster access to storage and more memory for its cache. It can run on 1 GB memory and 2 core CPU. 10 GB is a good start for storage. The Controller is less resource intensive, 1 CPU and 0.5 GB memory is a good start.