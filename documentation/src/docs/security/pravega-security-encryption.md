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
# Pravega Encryption

## Encryption of data in flight
Pravega ensures that all the data in flight can be passed by applying encryption.
The different [channels](https://github.com/pravega/pravega/wiki/PDP-23-%28Pravega-Security-Encryption-and-Role-Based-Access-Control%29)
can be configured with TLS and encryption can be enabled for them.

### Certificate Management
Pravega expects administrators and users to create and manage certificate creation, deployment and management.
Pravega provides various configuration parameters using which certificates for different communication channels can be specified.

### Encrypted data flow between Pravega client and Pravega Controller and Segment Store
Pravega uses same certificate to interact with the Controller and Segment Store. The certificates needs to be mentioned specifically on the client and the server machine.

**Note:** These certificates are not loaded from the truststore.

### Encrypted data flow between Pravega and Tier 1 (Apache Bookkeeper)
Pravega Segment Store uses Apache Bookkeeper as Tier 1 Storage. Apache Bookkeeper supports JKS based truststore. Segment Store uses JKS based truststore to interact with it. See, [configurations](pravega-security-configurations.md#pravega-segment-store) for more details.

### Encrypted access to Apache Zookeeper
Pravega Segment Store and Pravega Controller interact with Apache Zookeeper. These connections can be encrypted based on configuration.
The details about the configurations can be found at for [Pravega Segment Store](pravega-security-configurations.md#segment-store-tls-configuration-parameters) and for [Pravega Controller](pravega-security-configurations.md#controller-tls-configuration-parameters).

## Encryption of data at rest
### Encryption of data in Tier 1
Pravega uses Apache BookKeeper as Tier 1 implementation. Apache Bookkeeper currently does not support encryption of data written to disk.

### Encryption of data in Tier 2
Pravega can work with different storage options for Tier 2. To use any specific storage option, it is necessary to implement a storage interface. We currently have the following options implemented:

 - HDFS
 - Extended S3
 - File system (NFS).

Pravega does not encrypt data before storing it in Tier 2. Consequently, Tier 2 encryption is only an option in the case the storage system provides it natively, and as such, needs to be configured directly in the storage system, not via Pravega.
