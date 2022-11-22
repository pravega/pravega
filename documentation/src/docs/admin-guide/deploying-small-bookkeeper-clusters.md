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
# Trade-offs Deploying Small Bookkeeper Clusters for Pravega

[Apache Bookkeeper](https://bookkeeper.apache.org/) is the main 
Tier-1 implementation of Pravega. Often, users may deploy large 
Pravega clusters that involve instantiating multiple Bookies. 
In this scenario, we can safely assume that Segment Store instances 
could write data to a number of Bookies significantly larger than the 
`ensembleSize`, meaning that in the case of Bookie crashes, these can 
be replaced without impacting Pravega writes to Bookkeeper. Deployments 
in large on-premise clusters or in 
[public clouds](https://cncf.pravega.io/blog/2020/06/20/deploying-pravega-in-kubernetes/) 
are examples of this kind of deployment.

However, there may be cases in which users need to deploy Pravega
(and Bookkeeper) in small clusters (i.e., very limited number of nodes
and/or storage per node). Note that a "node" can be either a physical or
a virtual entity running Bookies. In this scenario, the decisions taken to 
deploy Bookkeeper and configure the Bookkeeper client in Pravega may have 
important implications on, at least, the following key aspects:

- _Tolerance to temporary failures_: Whether a temporary failure in a 
node or a Bookie implies that Pravega can continue reading (_R_) or writing
(_W_) data to Bookkeeper. For instance, if a given Bookkeeper cluster and 
client configuration can tolerate one temporary Bookie failure while writing
data, we could denote it as `1W/0R`. Similarly, if a client can read data
while a Bookie is unavailable, we denote it as `0W/1R`. This aspect is
relevant, as it determines whether Pravega can continue performing IO while
one or more Bookies/nodes are not reachable for some time.

- _Tolerance to permanent failures_: Apart from temporary unavailability of
Bookies and nodes, users may also face permanent crashes that imply the
loss of data of a given Bookie or even a whole node. This is a critical
aspect to take into account, as in general we should not allow data to 
be lost.

- _Performance_: Bookkeeper stores data using two main storage locations:
_journal_ and _ledger_ (we can leave _index_ aside in this discussion).
The main reason for having two storage locations for data is the different
types of workloads they exhibit (e.g., sequential, synchronous writes
vs buffered, random writes plus reads). This design decision is key
if using hard drives, as it allows us to use one dedicated drive per workload
type to improve performance. But even using modern SSD or NVMe drives for Bookies,
users need to decide whether to dedicate individual drives for journal and ledger
or collocating them in the same drive. This still plays a relevant role in performance.

In the table below, we have summarized the trade-offs that different deployment
options of Bookkeeper and client configuration may have in small clusters.
We highlight the following takeaways:
- In many cases, you need to chose between tolerating temporary write failure  
or reads failures. This is because for very small clusters, the replication factor
configured in the client is the same as the number of Bookies, so no write failure
is allowed (but we can still read data and tolerate permanent Bookie crashes).
- Some cluster layouts force users to trade-off between good performance 
(i.e., dedicating 1 drive to each journal and ledger) and tolerate permanent crashes.
- [Topology-awareness in Bookkeeper client](https://bookkeeper.apache.org/docs/latest/api/javadoc/org/apache/bookkeeper/client/EnsemblePlacementPolicy.html)
is very important to prevent data loss when we have very few nodes to run Bookies.


![Trade-off Deploying Small Bookkeeper Clusters for Pravega](../img/bookkeeper-small-deployment-guideline.png)

