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
# Pravega API Reference

Pravega provides applications with a reliable, fast and consistent storage service for data Streams.
Along the years, Pravega has also incorporated powerful APIs on top of the Stream primitive that allows
applications to have an _all-in-one storage service_ (streams, batch jobs, store K/V pairs, etc.). 
We believe that providing developers rich APIs over the same storage system this is key, as it removes
the need to deploy one storage service per use-case. At the moment, the Pravega Client API provides the 
following abstractions:

## Event Stream Writer/Reader
[Event Stream Writer](https://www.pravega.io/docs/latest/javadoc/clients/io/pravega/client/stream/EventStreamWriter.html)
and  [Event Stream Reader](https://www.pravega.io/docs/latest/javadoc/clients/io/pravega/client/stream/EventStreamReader.html)
provide a Java library that implements a convenient API for Writer and Reader streaming applications, respectively.
The client libraries encapsulate the [Wire Protocol](https://github.com/amit-kumar59/pravega/blob/Issue-8163-pravega-broken-urls/documentation/src/docs/wire-protocol.md)
that is used to convey requests and responses between Pravega clients and the Pravega service.
Note that for writing events using [Transactions](https://github.com/amit-kumar59/pravega/blob/Issue-8163-pravega-broken-urls/documentation/src/docs/pravega-concepts.md/#transactions)
to achieve exactly-once semantics, Pravega offers the [Transactional Event Stream Writer](https://www.pravega.io/docs/latest/javadoc/clients/io/pravega/client/stream/TransactionalEventStreamWriter.html).

## Batch Client
_[Batch Client](https://cncf.pravega.io/docs/latest/javadoc/clients/io/pravega/client/BatchClientFactory.html)_, instead of processing a stream as data comes in, 
enables applications to read data from all segments (irrespective of time) in parallel, in order to more efficiently
perform batch processing over historical data. For example, say a stream starts with one segment S_1, and at time T_1 it scales up and splits the one segment into two new segments S_2 and S_3. When the stream is sealed, the S_2 and S_3 are the last segments. A batch read of this stream consists of reading S_1, S_2, and S_3 in parallel.

The Batch Client allows:
- listing the segments in a Stream between two StreamCuts.
- creating an iterator to read the contents of a segment.
- generating a streamCut that is a bounded distance from another streamCut.

## Byte Client
_[Byte Client](https://www.pravega.io/docs/latest/javadoc/clients/io/pravega/client/ByteStreamClientFactory.html)_ is a byte-oriented API providing a way 
to write data to Pravega without writing events. It presents an interface of a InputStream and an OutputStream. Data 
written in this way is not framed or interpreted by Pravega. So there are no length headers or event boundaries. As 
such byte offsets into the stream are meaningful and directly exposed.

## KV Tables Client
_[KV Tables Client](https://www.pravega.io/docs/latest/javadoc/clients/io/pravega/client/tables/package-summary.html)_: Real-world analytical 
pipelines are seldom made up of just Stream Processing. Many applications 
require storing and retrieving state, and some applications even need to store results in some sort of database 
(whether structured or not). Such applications require complex deployments involving multiple different types of 
storage systems, each having its own deployment and runtime requirements. This can easily lead to having significant 
operational burdens. We therefore propose the implementation of Key-Value Tables (KVT): a new primitive that organizes 
data as indexed Key-Value Pairs (KVPs) using nothing else but existing features already implemented in Pravega at 
this time. A KVT is a distributed Key-Value Store that associates arbitrary Keys to Values and enables common 
operations on them, such as insertion, retrieval and removal. By providing Key-Value Tables as another primitive 
in Pravega alongside Streams, we aim to simplify the operational burden of analytical pipelines by not having to 
maintain disparate storage systems - both KVTs and Streams share the same underlying infrastructure powered by 
Pravega Segments.

## State Synchronizer Client
_[State Synchronizer Client](https://www.pravega.io/docs/latest/javadoc/clients/io/pravega/client/state/StateSynchronizer.html)_:
In many situations, a distributed application may need to share information across multiple processes that may
update it (e.g., configuration file). In this scenario, keep the data consistent under concurrent updates is key.
The State Synchronizer Client just solves this problem: it provides a means to have state that is synchronized between 
many processes.

## Javadoc
For full details on the available APIs, please check out the [Pravega Client Javadoc](https://www.pravega.io/docs/latest/javadoc/clients/).
