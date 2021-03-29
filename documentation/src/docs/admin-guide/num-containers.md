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

# Setting the Number of Segment Containers

[Segment Containers](http://pravega.io/docs/latest/segment-containers/) are the unit of IO parallelism in a Pravega 
Cluster. Therefore, we need to set this parameter carefully in a production environment. The relevant parameters
in Pravega to reason about setting the right number of Segment Containers are the following:

- **`pravegaservice.container.count`, `controller.container.count`**: Number of Segment Containers in the system. 
This value must be the same across all SegmentStore instances in this cluster. This value cannot be changed dynamically. 
See documentation for details. Valid values: Positive integer.
_Type_: `Integer`. _Default_: `4`. _Update-mode_: `read-only`.

- **`pravegaservice.threadPool.core.size`**: Maximum number of threads in the Core SegmentStore Thread Pool. This pool 
is used for all SegmentStore-related activities, except Netty-related tasks and Long Term Storage activities. Examples 
include: handling inbound requests, processing reads, background maintenance operations and background operation processing.
Valid values: Positive integer.
_Type_: `Integer`. _Default_: `30`. _Update-mode_: `per-server`.

As Segment Containers define the IO parallelism in the system, this yields to consider two main aspects when choosing 
the right number of Segment Containers: 

- _Compute resources per Segment Store_: Each Segment Container runs a set of services that consume CPU power. 
This means that the number of Segment Containers in the system is limited by the available compute power in the cluster.
As a rule of thumb, we can define a number of Segment Containers per Segment Store according to the number of cores
per instance (e.g., 1 or 2 Segment Containers per core, depending on the core power). Similarly, the size of the thread
pool (`pravegaservice.threadPool.core.size`) should be sized according to the number of segment containers 
(e.g., 2-4 threads per Segment Container, with a minimum number of 20 threads). Note that setting a value of Segment
Containers and/or threads higher than necessary may lead to performance degradation due to CPU contention under
intense workloads.

- _Initial number of Segment Stores_: This deployment decision plays a central role when it comes to define the number 
of Segment Containers in the system and may vary depending on the deployment scenario. On the one hand, an on-premise 
environment we may have a fixed amount of hardware available to run Pravega, in which case the number of Segment Store 
instances may be known. On the other hand, in a cloud environment a reasonable approach would be to right-size the initial 
number of Segment Store instances based on the expected load. We have published performance analysis that can help users 
to understand the load that a Pravega Cluster can sustain, as well as the associated latency numbers 
[[1]](https://blog.pravega.io/2020/10/01/when-speeding-makes-sense-fast-consistent-durable-and-scalable-streaming-data-with-pravega/), 
[[2]](https://blog.pravega.io/2021/03/10/when-speed-meets-parallelism-pravega-performance-under-parallel-streaming-workloads/). 
In addition to that, we also provide benchmark tools that can help users to generate loads that resemble that of users
to infer the right number of Segment Store instance that can reasonably absorb a given workload
(e.g., [Pravega Benchmark](https://github.com/pravega/pravega-benchmark), 
[OpenMessagin Benchmark Pravega Driver](https://github.com/pravega/openmessaging-benchmark)). We also have developed
a [provisioner tool for Pravega](https://github.com/pravega/pravega-tools/tree/master/pravega-provisioner) to help you 
reason about these aspects.

With the previous points in mind, we can define the number of Segment Containers in the system 
(i.e., `pravegaservice.container.count`, `controller.container.count`) by just multiplying the number of Segment Containers 
per instance by the number of initial instances. Let's see a practical example. Image that we want to deploy a Pravega 
Cluster using AWS EKS instances with 8 vCPUs and 16GB of RAM for running Segment Stores. If we assume that the normal load
in the system to be 50MBps (1KB events), then we can safely assume that 3 Segment Stores may be enough to absorb it.
Given that, we could suggest a configuration as follows:

- `[pravegaservice.container.count|controller.container.count]=24`. The instances in our example have 8 vCPUs. Then,
we defined to use 1 vCPU Segment Container and we multiplied this by the initial number of Segment Stores (3 instances).
With this, the total number of Segment Containers in the system is set to 24.

- `pravegaservice.threadPool.core.size=20`. In a Kubernetes environment, we do not expect cores to be as powerful as in 
plain VMs or bare metal environments (e.g., additional levels of virtualization, many services sharing the same 
virtual/physical resources). For this reason, we can assume that having 2 threads per container may be enough,
and given that 8 Segment Containers per Segment Store x 2 threads per Segment Container is lower than the minimum
number of threads recommended (i.e., 20 threads), we just keep this setting to 20.


There is one final aspect to consider in a production Pravega Cluster: _scalability_. In our example above, we have 
mentioned that the expected workload was 50MBps and 3 Segment Stores may be enough to handle it. However, workloads may 
grow unexpectedly, and we need our cluster to be able to keep up. The number of Segment Containers in the system _cannot
be modified_: this means that it sets an upper bound to the maximum usable size of the Pravega Cluster. That is, 
with 24 Segment Containers in the system, we would be able to scale from 3 Segment Stores (running 8 Segment 
Containers each) to 24 Segment Stores (running 1 Segment Container each). Clearly, this would greatly increase 
the IO capacity of our Pravega Cluster for absorbing a load peak, and then srink the cluster when the burst is over. 
Still, is important to note that adding Segment Stores beyond 24 instances would be useless, as these extra instances 
would not have any Segment Container assigned to run, which means that they would not be able to run IO. Therefore, we 
need to consider that the  parameter `[pravegaservice.container.count|controller.container.count]` is also imposing a 
limit on the maximum usable size of the Pravega Cluster.   