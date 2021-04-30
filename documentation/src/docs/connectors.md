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
# Pravega Connectors

Connectors allow integrating Pravega with different data sources and sinks.

## Supported connectors
Currently, Pravega offers the following connectors:  

- [Flink Connector](https://github.com/pravega/flink-connectors): The Flink Connector enables building 
end-to-end stream processing pipelines with Pravega in [Apache Flink](https://flink.apache.org/). This also allows 
reading and writing data to external data sources and sinks via Flink Connector.

- [Spark Connector](https://github.com/pravega/spark-connectors): Connector to read and write Pravega Streams with 
[Apache Spark](http://spark.apache.org/), a high-performance analytics engine for batch and streaming data.
 The connector can be used to build end-to-end stream processing pipelines that use Pravega as the stream storage and message bus, 
and Apache Spark for computation over the streams.

- [Hadoop Connector](https://github.com/pravega/hadoop-connectors): Implements both the input and the output format 
interfaces for [Apache Hadoop](https://hadoop.apache.org/). It leverages Pravega batch client to read existing events in parallel; 
and uses write API to write events to Pravega streams.

- [Presto Connector](https://github.com/pravega/presto-connector): [Presto](https://prestodb.io) is a distributed SQL
query engine for big data. Presto uses connectors to query storage from different storage sources. 
This connector allows Presto to query storage from Pravega streams.

- [Boomi Connector](https://github.com/pravega/boomi-connector): A Pravega connector for the 
[Boomi Atomsphere](https://boomi.com/platform/integration/applications/).

- [Nifi Connector](https://github.com/pravega/nifi-pravega): Connector to read and write Pravega streams 
with [Apache NiFi](https://nifi.apache.org/).


## Third-party contributions
In addition to the connectors provided by the Pravega organization, open-source contributors have also
created connectors for external projects:

- [Alpakka connector](https://doc.akka.io/docs/alpakka/current/pravega.html): 
The [Alpakka](https://github.com/akka/alpakka) project is an open source initiative to 
implement stream-aware, reactive, integration pipelines for Java and Scala. It is built on top of Akka Streams, 
and has been designed from the ground up to understand streaming natively and provide a DSL for reactive and 
stream-oriented programming, with built-in support for back-pressure. This Alpakka Pravega connector lets you connect 
Pravega to Akka Streams.

- [Pravega Spring Cloud Stream Binder](https://github.com/gustavomzw/pravega-binder):
[Spring Cloud Stream](https://spring.io/projects/spring-cloud-stream) is a framework for building highly scalable
event-driven microservices connected with shared messaging systems. The Pravega Binder connects Pravega to
[Spring Cloud Data Flow](https://spring.io/projects/spring-cloud-dataflow), which provides tools to create complex
topologies for streaming and batch data pipelines. The data pipelines consist of Spring Boot apps, built using the
Spring Cloud Stream or Spring Cloud Task microservice frameworks.

- [Memstate Connector](https://github.com/DevrexLabs/memstate/tree/master/src/Memstate.Pravega): 
[Memstate](https://memstate.io/) is an in-memory event-sourced ACID-transactional replicated object graph engine. 
The Pravega connector allows Memstate to use it as storage provider for persistence and global message ordering.
