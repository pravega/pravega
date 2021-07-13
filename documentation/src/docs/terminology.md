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
# Terminology

The glossary of terms related to Pravega is given below:

| **Term**                        | **Definition**                                                                                                                                            |
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Pravega**                     | Pravega is an open source storage system that exposes **stream** as the main primitive for continuous and unbounded data.                                                                                    |
| **Stream**                      | A durable, elastic, append-only, unbounded sequence of bytes that has good performance and strong consistency.                                                                                                |
|                                 | A **Stream** is identified by a **Stream name** and a **Scope**.                                                                                                         |
|                                 | A **Stream** is comprised of one or more **Stream Segments.**                                                                                             |
| **Stream Segment**              | A shard of a **Stream**.                                                                                                                                  |
|                                 | The number of **Stream Segments** in a **Stream** might vary over time according to load and **Scaling Policy**.                                          |
|                                 | In the absence of a **Scale Event**, **Events** written to a **Stream** with the same **Routing Key** are stored in the same **Stream Segment** and are ordered.                                                                                                                                                           |
|                                 | When a **Scale Event** occurs, the set of **Stream Segments** of a **Stream** changes and **Events** written with a given **Routing Key** *K* before the **Scaling Event** are stored in a different **Stream Segment** compared to **Events** written with the same **Routing Key** *K* after the event.                                                                                                                                                          |
|                                 | In conjunction with **Reader Groups**, the number of **Stream Segments** is the maximum amount of read parallelism of a **Stream**.                                                                                                                                                         |
| **Scope**                       | A namespace for **Stream** names.                                                                                                                         |
|                                 | A **Stream** name must be unique within a **Scope**.                                                                                                                                                          |
| **Event**                       | A collection of bytes within a **Stream.**                                                                                                                |
|                                 | An **Event** is associated with a **Routing Key.**                                                                                                                                                          |
| **Routing Key**                 | A property of an **Event** used to route messages to **Readers.**                                                                                         |
|                                 | Two **Events** with the same **Routing Key** will be read by **Readers** in exactly the same order they were written.                                                                                                                                                          |
| **Reader**                      | A software application that reads data from one or more **Streams**.                                                                                      |
| **Writer**                      | A software application that writes data to one or more **Streams.**                                                                                       |
| **Pravega Java Client Library** | A Java library used by applications to interface with **Pravega**                                                                                        |
| **Reader Group**                | A named collection of one or more **Readers** that read from a **Stream** in parallel.                                                                    |
|                                 | **Pravega** assigns **Stream Segments** to the **Readers** ensuring that all **Stream Segments** are assigned to at least one **Reader** and that they are balanced across the **Readers**.                                                                                                                                                          |
| **Position**                    | An offset within a **Stream**, representing a type of recovery point for a **Reader**.                                                                    |
|                                 | If a **Reader** crashes, a **Position** can be used to initialize the failed **Reader**'s replacement so that the replaced **Reader** resumes processing the **Stream** from where the failed **Reader** left off.                                                                                                                                                          |
| **Tier 1 Storage**              | Short term, low-latency, data storage that guarantees the durability of data written to **Streams**.                                                      |
|                                 | The current implementation of **Tier 1** uses  [Apache Bookkeeper](http://bookkeeper.apache.org/).                                                                                                                                                          |
|                                 | **Tier 1** storage keeps the most recent appends to streams in **Pravega.**                                                                                                                                                          |
|                                 | As data in **Tier 1** ages, it is moved out of **Tier 1** into **Tier 2.**                                                                                                                                                          |
| **Tier 2 Storage**              | A portion of **Pravega** storage based on cheap and deep persistent storage technology such as [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html), [DellEMC's Isilon](https://www.dellemc.com/en-us/storage/isilon/index.htm#collapse) or [DellEMC's Elastic Cloud Storage](https://www.dellemc.com/en-us/storage/ecs/index.htm#collapse). |
| **Pravega Server**              | A component of **Pravega** that implements the **Pravega** data plane API for operations such as reading from and writing to **Streams**.         |
|                                 | The data plane of **Pravega,** also called the **Segment Store,** is composed of one or more **Pravega Server** instances.                                                                                                                                                          |
| **Segment Store**               | A collection of **Pravega Servers** that in aggregate form the data plane of a Pravega cluster.                                                               |
| **Controller**                  | A component of Pravega that implements the **Pravega** control plane API for operations such as creating and retrieving information about **Streams**.    |
|                                 | The control plane of **Pravega** is composed of one or more **Controller** instances coordinated by [Zookeeper](https://zookeeper.apache.org/).                                                                                                                                                          |
| **Auto Scaling**                | A Pravega concept that allows the number of **Stream Segments** in a **Stream** to change over time, based on **Scaling Policy.**                         |
| **Scaling Policy**              | A configuration item of a **Stream** that determines how the number of **Stream Segments** in the **Stream** should change over time.                     |
|                                 | There are three kinds of **Scaling Policy**, a **Stream** has exactly one of the following at any given time.                                                                                                                                                          |
|                                 | - Fixed number of **Stream Segments**                                                                                                                                                        |
|                                 | - Change the number of **Stream Segments** based on the number of bytes per second written to the **Stream** (Size- based)                                                                                                                                                       |
|                                 | - Change the number of **Stream Segments** based on the number of **Events** per second written to the **Stream** (Event-based)                                                                                                                                                        |
| **Scale Event**                 | There are two types of **Scale Event**: Scale-Up Event and Scale-Down Event. A **Scale Event** triggers **Auto Scaling**.                                     |
|                                 | A Scale-Up Event occurs when there is an increase in load, the number of **Stream Segments** are increased by splitting one or more **Stream Segments** in the **Stream**.                                                                                                                                                           |
|                                 | A Scale-Down Event occurs when there is a decrease in load, the number of  **Stream Segments** are reduced by merging one or more **Stream Segments** in the **Stream**.                                                                                                                                                          |
| **Transaction**                 | A collection of **Stream** write operations that are applied atomically to the **Stream**.                                                                |
|                                 | Either all of the bytes in a **Transaction** are written to the **Stream** or none of them are.                                                                                                                                                          |
| **State Synchronizer**          | An abstraction built on top of **Pravega** to enable the implementation of replicated state using a Pravega **segment** to back up the state transformations.     |
|                                 | A **State Synchronizer** allows a piece of data to be shared between multiple processes with strong consistency and optimistic concurrency.                                                                                                                                                          |
| **Checkpoint**                  | A kind of **Event** that signals all **Readers** within a **Reader Group** to persist their state.                                                        |
| **StreamCut**                   | A **StreamCut** represents a consistent position in the **Stream**. It contains a set of **Segment** and offset pairs for a single **Stream** which represents the complete keyspace at a given point in time. The offset always points to the event boundary and hence there will be no offset pointing to an incomplete **Event**. |
