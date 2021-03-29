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

# Operator Ecosystem

A Pravega Cluster is formed by Pravega services (Controller, Segment Store), but it also requires to run other 
services to work: [Zookeeper](https://zookeeper.apache.org/) (consensus service) and 
[Bookkeeper](https://bookkeeper.apache.org/) (durable log). For this reason, the Pravega project has contributed 
open-source Kubernetes Operators to deploy these services as well. 
The available operators under the Pravega organizations are:

- [Zookeeper Operator](https://github.com/pravega/zookeeper-operator)

- [Bookkeeper Operator](https://github.com/pravega/bookkeeper-operator) 

- [Pravega Operator](https://github.com/pravega/pravega-operator)