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

## Event Stream Client
_[Event Stream Client](pravega-concepts.md/#Streams)_
provides a Java library that implements a convenient API for Writer and Reader streaming applications to use. The 
client library encapsulates the [Wire Protocol](wire-protocol.md) that is used to convey requests and responses 
between Pravega clients and the Pravega service. Note that the Event Stream Client can manage events directly on Streams
or via [Transactions](pravega-concepts.md/#transactions) to achieve exactly-once semantics.

## Batch Client
_[Batch Client](clients-and-streams.md/#Batch client API overview)_, instead of processing a stream as data comes in, 
enables applications to read data from all segments (irrespective of time) in parallel, in order to more efficiently
perform batch processing over historical data. The Batch Client allows listing the segments in a Stream and creating 
an iterator on any give segment to read the contents of the segment.

## Byte Client
_[Byte Client](https://github.com/pravega/pravega/wiki/PDP-30-(ByteStream-API))_ is a byte-oriented API providing a way 
to write data to Pravega without writing events. It presents an interface of a InputStream and an OutputStream. Data 
written in this way is not framed or interpreted by Pravega. So there are no length headers or event boundaries. As 
such byte offsets into the stream are meaningful and directly exposed.

## KV Tables Client
_[KV Tables Client](https://github.com/pravega/pravega/wiki/PDP-39-(Key-Value-Tables-Beta-1))_: Real-world analytical 
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
_[State Synchronizer Client](state-synchronizer.md)_:
In many situations, a distributed application may need to share information across multiple processes that may
update it (e.g., configuration file). In this scenario, keep the data consistent under concurrent updates is key.
The State Synchronizer Client just solves this problem: it provides a means to have state that is synchronized between 
many processes.

## Javadoc
For full details on the available APIs, please check out the [Pravega Client Javadoc](clients/index.html).
