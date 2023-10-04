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

# Future Items
The following items are new features that we wish to build in upcoming Pravega releases, however many active work is currently underway.  Please reach out on the Pravega channels if you're interested in picking one of these up.

-  Operational Features
    -  Non-disruptive and rolling upgrades for Pravega
    -  Provide default Failure Detector
    -  Exposing information for administration purposes
    -  Ability to define throughput quotas and other QoS guarantees
-  Pravega Connectors / Integration
    -  Kafka API Compatibility (Producer and Consumer APIs)
    -  Spark connectors (source/sink)
    -  REST Proxy for Reader/Writer (REST proxy for Admin operations is already there)
-  Stream Management
    -  Stream aliasing
    -  Ability to assign arbitrary Key-Value pairs to streams - Tagging
-  Tiering Support
    -  Policy driven tiering of Streams from Streaming Storage to Long-term storage
    -  Support for additional Tier 2 Storage backends
