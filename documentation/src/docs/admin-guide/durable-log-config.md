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

# Durable Log Configuration: Apache Bookkeeper

Pravega guarantees that every acknowledged written event is durably stored and replicated. This is possible 
thanks to the durable log abstraction that the Segment Store offers, which relies on Apache Bookkeeper.
Therefore, the configuration of both the Bookkeeper client at the Segment Store and the configuration of 
Apache Bookkeeper service itself are critical for a production cluster. Note that we do not attempt to fully cover 
the configuration of Bookkeeper ([available here](https://bookkeeper.apache.org/docs/reference/config/)). Instead, 
we focus on the parameters that we have found important to properly configure in our practical experience:

- **`bookkeeper.ensemble.size`**: Ensemble size for Bookkeeper ledgers. This value need not be the same for all 
Pravega SegmentStore instances in this cluster, but it highly recommended for consistency. 
_Type_: `Integer`. _Default_: `3`. _Update-mode_: `per-server`.

- **`bookkeeper.ack.quorum.size`**: Write Ack Quorum size for Bookkeeper ledgers. This value need not be the same 
for all Pravega SegmentStore instances in this cluster, but it highly recommended for consistency.
_Type_: `Integer`. _Default_: `3`. _Update-mode_: `per-server`.

- **`bookkeeper.write.quorum.size`**: Write Quorum size for Bookkeeper ledgers. This value need not be the same for all 
Pravega SegmentStore instances in this cluster, but it highly recommended for consistency.
_Type_: `Integer`. _Default_: `3`. _Update-mode_: `per-server`.

- **`-Xmx`** (BOOKKEEPER JVM SETTING): Defines the maximum heap memory size for the JVM. 

- **`-XX:MaxDirectMemorySize`** (BOOKKEEPER JVM SETTING): Defines the maximum amount of direct memory for the JVM.

- **`ledgerStorageClass`** (BOOKKEEPER SETTING): Ledger storage implementation class.
_Type_: `Option`. _Default_: `org.apache.bookkeeper.bookie.SortedLedgerStorage`. _Update-mode_: `read-only`.

First, let's focus on the configuration of the _Bookkeeper client in the Segment Store_. The parameters
`bookkeeper.write.quorum.size` and `bookkeeper.ack.quorum.size` determine the 
[level of durability](https://bookkeeper.apache.org/docs/development/protocol/) (i.e., replicas) 
for data stored in the durable log until it is moved to long-term storage. For context, these parameters dictate
the upper (`bookkeeper.write.quorum.size`) and lower (`bookkeeper.ack.quorum.size`) bounds of the degree of replication
for your data in Bookkeeper. While the Bookkeeper client will attempt to write `bookkeeper.ack.quorum.size` copies
of each write, it will only write for `bookkeeper.ack.quorum.size` acknowledgements to proceed with the next write.
Therefore, it only guarantees that each and every write has at least `bookkeeper.ack.quorum.size` replicas, despite
many of them can have up to `bookkeeper.write.quorum.size` replicas. This is important to consider when reasoning 
about the expected number of replicas of our data. But there is another important aspect to consider when setting
these parameters: _stability_. Bookkeeper servers perform various background tasks, including ledger re-replication,
auditing and garbage collection cycles. If our Pravega Cluster is under heavy load and Bookkeeper servers are close
to saturation, it may be the case that one Bookkeeper server processes requests at a lower rate than others while
the mentioned background tasks are running. In this case, setting `bookkeeper.ack.quorum.size < bookkeeper.write.quorum.size` 
may lead to overload the slowest Bookkeeper server, as the client does not wait for its acknowledgements to continue writing data.
For this reason, in a production cluster we recommend to configuring:
- `bookkeeper.ack.quorum.size` = `bookkeeper.write.quorum.size` = 3: We define the number of acknowledgements equal
to the write quorum, which leads the Bookkeeper client to wait for all Bookkeeper servers to confirm every write.
This decision trades-off some write latency penalty in exchange of stability, which is reasonable in a production
environment. Also, we recommend setting 3 replicas as it is the de-facto standard to guarantee durability.

Another relevant configuration parameter in the Bookkeeper client is `bookkeeper.ensemble.size`, as it
determines the failure tolerance for Bookkeeper servers. That is, if we instantiate `N` Bookkeeper servers in
our Pravega Cluster, the Segment Store will be able to continue writing even if `N - bookkeeper.ensemble.size`
Bookkeeper servers fail. In other words, as long as there are `bookkeeper.ensemble.size` Bookkeeper servers
available, Pravega will be able to accept writes from applications. To this end, to maximize failure tolerance,
we suggest to keep `bookkeeper.ensemble.size` at the minimum value possible:

- `bookkeeper.ensemble.size` = `bookkeeper.ack.quorum.size` = `bookkeeper.write.quorum.size` = 3: This configuration
ensures 3-way replication per write, prevents overloading a slow Bookkeeper server, and tolerates the highest
number of Bookkeeper server failures.

While there are many aspects to consider in the configuration of Bookkeeper, we highlight the following ones:

- _Bookkeeper memory_: In production, we recommend at least 4GB of JVM Heap memory (`-Xmx=4g`) and 4GB of JVM Direct
Memory (`-XX:MaxDirectMemorySize=4g`) for Bookkeeper servers. Direct memory is especially important, as Netty 
(internally used by Bookkeeper) may need a significant amount of memory to allocate buffers.

- _Ledger storage implementation_: Bookkeeper offers different implementations to store ledgers' data. In Pravega,
we recommend using the simplest ledger storage implementation: the Interleaved Ledger Storage
(`ledgerStorageClass=org.apache.bookkeeper.bookie.InterleavedLedgerStorage`). There are two main reasons that
justify this decision: i) Pravega does not read from Bookkeeper in normal conditions, just upon a [Segment
Container recovery](http://pravega.io/docs/latest/segment-store-service/#container-startup-normalrecovery). 
This means that Pravega does not need any extra complexity associated to optimize ledger
reads in Bookkeeper. ii) We have observed a lower resource usage and better stability in Bookkeeper when 
using Interleaved Ledger Storage compared to 
DBLedger[[2]](https://blog.pravega.io/2021/03/10/when-speed-meets-parallelism-pravega-performance-under-parallel-streaming-workloads/).
