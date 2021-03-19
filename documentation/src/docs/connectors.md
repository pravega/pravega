<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Connectors Ecosystem

Connectors allow integrating Pravega with different data sources and sinks.
Currently, Pravega offers the following connectors:  

- [Flink Connector](flink-connectors/documentation/src/docs/index.md): The Flink Connector enables building 
end-to-end stream processing pipelines with Pravega in [Apache Flink](https://flink.apache.org/). This also allows 
reading and writing data to external data sources and sinks via Flink Connector.

- [Hadoop Connector](https://github.com/pravega/hadoop-connectors): Implements both the input and the output format 
interfaces for [Apache Hadoop](https://hadoop.apache.org/). It leverages Pravega batch client to read existing events in parallel; 
and uses write API to write events to Pravega streams.

- [Spark Connector](https://github.com/pravega/spark-connectors): Connector to read and write Pravega Streams with 
[Apache Spark](http://spark.apache.org/), a high-performance analytics engine for batch and streaming data.
 The connector can be used to build end-to-end stream processing pipelines that use Pravega as the stream storage and message bus, 
and Apache Spark for computation over the streams.

- [Presto Connector](https://github.com/pravega/presto-connector): [Presto](ttps://prestodb.io) is a distributed SQL 
query engine for big data. Presto uses connectors to query storage from different storage sources. 
This connector allows Presto to query storage from Pravega streams.

- [Boomi Connector](https://github.com/pravega/boomi-connector): A Pravega connector for the 
[Boomi Atomsphere](https://boomi.com/platform/integration/applications/).



