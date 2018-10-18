<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Encryption

## Encryption of data in flight
Pravega ensures that all the data in flight can be passed by applying encryption.
The different [channels](https://github.com/pravega/pravega/wiki/PDP-23:-Pravega-security----encryption-and-Role-Based-Access-Control#b-encryption-of-data-in-flight-over-network-and-in-tier-1)
can be configured with TLS and encryption can be enabled for them.

### Certificate Management
Pravega expects administrators and users to create and manage certificate creation, deployment and management.
Pravega provides various configuration parameters using which certificates for different communication channels can be specified.

### Encrypted data flow between Pravega client and Pravega controller and segment store
Pravega uses same certificate to interact with the controller and segment store. The certificates needs to be mentioned specifically on the client and the server machine.

**Note:** These certificates are not loaded from the truststore.

### Encrypted data flow between Pravega and Tier 1 (Apache Bookkeeper)
Pravega segment store uses Apache Bookkeeper as Tier 1 storage. Apache Bookkeeper supports JKS based truststore. Segment store uses JKS based truststore to interact with it.
The configurations can be found [here](pravega-security-configurations.md#pravega-segment-store).

### Encrypted access to Apache Zookeeper
This implementation is still in [progress](https://github.com/pravega/pravega/issues/2034).

## Encryption of data at rest
### Encryption of data in Tier 1
Pravega uses Apache BookKeeper as Tier 1 implementation. Apache Bookkeeper currently does not support encryption of data written to disk.

### Encryption of data in Tier 2
Pravega can work with different storage options for Tier 2. To use any specific storage option, it is necessary to implement a storage interface. We currently have the following options implemented: HDFS, extended S3 and file system (NFS).

Pravega does not encrypt data before storing it in Tier 2. Consequently, Tier 2 encryption is only an option in the case the storage system provides it natively, and as such, needs to be configured directly in the storage system, not via Pravega.
