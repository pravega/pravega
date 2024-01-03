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

# Roadmap
The following items are new features that we wish to build in upcoming Pravega releases. 
Please reach out on the Pravega channels if you're interested in picking one of these up.

**Reduce and control Pravega resources:**    
  -  Minimize resource footprint for Edge environments.    
  -  Plugable consensus service (Zookeeper, Etcd).
  -  Ability to define throughput quotas and other QoS guarantees.

**Enrich Pravega ecosystem and language support:**
  -  Kafka compatibility.
  -  Sensor collector improvements.
  -  Data replicator.
  -  New client bindings and add capabilities to existing ones (e.g., NodeJS, Go, C#).

**Flexible management of Pravega streams:**
  -  Zone/region awareness in Pravega.

**Improve Pravega's Long-Term Storage support:**
  -  Policy-driven tiering configuration for Streams/Scopes.
  -  Support for additional Long-Term Storage backends.
  -  Allow Bookkeeper-only deployment (for both Write-Ahead Log and Long-Term Storage).

**Increase support for data types and data reduction:**
  -  Configurable Long-Term Storage formats.
  -  New serializers (compression, specialized data formats).

