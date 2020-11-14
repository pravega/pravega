<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Roadmap

# Version 0.3
The following will be the primary feature focus areas for our upcoming release.

## Retention Policy Implementation
Retention policies allow an operator to define a specific Stream size or data age.  Any data beyond this threshold will be automatically purged.

## Transactions API
The current transactions API is functional, however it is cumbersome and requires detailed knowledge of Pravega to configure appropriate values such as timeouts.  This work will simplify the API and automate as many timeouts as possible.

## Exactly Once Guarantees
Focus is on testing and strengthening exactly once guarantees and correctness under failure conditions.

## Low-level Reader API
This will expose a new low-level reader API that provides access the low level byte stream as opposed to the event semantics offered by the Java Client API.  This can also be leveraged in the future to build different flavors of reader groups.

## Security
Security for this release will focus on support for securing Pravega external interfaces along with basic access controls on stream access and administration.
-  Access Control on Stream operations.
-  Auth between Clients and Controller/SegmentStore.
-  Auth between SegmentStore and Tier 2 Storage.

## Pravega Connectors
Pravega ecosystem interconnectivity will be augmented with the following:
-  Expanded Flink connector support (batch & table API support).
-  Logstash connector.
-  Others also under consideration (Interested in writing a connector? Talk to us on [Slack](https://pravega-io.slack.com/))



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
  -  Ability to logically group multiple Streams
  -  Ability to assign arbitrary Key-Value pairs to streams - Tagging
-  Tiering Support
  -  Policy driven tiering of Streams from Streaming Storage to Long-term storage
  -  Support for additional Tier 2 Storage backends
