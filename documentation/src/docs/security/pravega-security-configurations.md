<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Security Configurations

This document describes the list of security configuration parameters for Pravega components.

## Security Configuration Parameters in Distributed Mode

This section lists the security configuration parameters for Segment Store and Controller in a distributed mode deployment.

### Segment store <a name = "pravega-segment-store"></a>

|Parameter|Description|Default Value|Feature|
|---------|-------|-------------|------------|
| pravegaservice.enableTls | Whether to enable TLS for client-server communications. | False | TLS |
| pravegaservice.certFile | Path of the Certificate file containing the public key, to be used for TLS. | Empty | TLS |
| pravegaservice.keyFile | Path of the file containing the private key, to be used for TLS. | Empty | TLS |
| pravegaservice.secureZK | Whether to enable TLS for communication with Apache Zookeeper. | False | TLS |
| pravegaservice.zkTrustStore | Path of the truststore file to be used for communicating with Apache Zookeeer. | Empty | TLS |
| pravegaservice.zkTrustStorePasswordPath | Path of the file containing the password of the truststore. | Empty | TLS |
| autoScale.tlsEnabled | Whether to enable TLS for internal communication between segment store and controller. | False | TLS |
| autoScale.tlsCertFile | Path of the certificate file used for TLS-enabled internal communication between segment store and controller. | Empty | TLS |
| autoScale.validateHostName | Whether to enable hostname verification during TLS-enabled internal communication between segment store and controller. | True | TLS |
| autoScale.authEnabled | Whether to enable authorization and authentication for internal communication with controller. | False | Auth |
| autoScale.tokenSigningKey | The signing key used to sign the delegation token sent from controller to segment store. | Empty | Auth |
| bookkeeper.tlsEnabled | Whether to enable TLS for communication between segment store and Apache Bookkeeper. | False | TLS |
| bookkeeper.tlsTrustStorePath | Path of the truststore file to be used for communicating with Apache Bookkeeper. | Empty | TLS |


### Controller <a name ="pravega-controller"></a>

|Parameter|Details|Default Value|Feature|
|---------|-------|-------------|-------|
| controller.auth.tlsEnabled | Whether to enable TLS for client-server communications. | False | TLS |
| controller.auth.tlsCertFile | Path of the Certificate file containing the public key, to be used for TLS. | Empty | TLS |
| controller.auth.tlsKeyFile | Path of the file containing the private key, to be used for TLS. | Empty | TLS |
| controller.auth.tlsTrustStore | Path of the truststore file, to be used for communicating with segment stores. | Empty | TLS |
| controller.rest.tlsKeyStoreFile | Path of the keystore file in `.jks` format, to be used for REST API | Empty | TLS |
| controller.rest.tlsKeyStorePasswordFile | Path of the file containing the REST API's keystore password. | Empty | TLS |
| controller.zk.secureConnection | Whether to enable TLS for communication with Apache Zookeeper| False | TLS |
| controller.zk.tlsTrustStoreFile | Path of the truststore file to be used communicating with Apache Zookeeer. | Empty | TLS |
| controller.zk.tlsTrustStorePasswordFile | Path of the file containing the password of the truststore. | Empty | TLS |
| controller.auth.enabled | Whether to enable authentication and authorization for clients. | False | Auth |
| controller.auth.userPasswordFile | Path of the file containing user credentials and ACLs, to be used by the PasswordAuthHandler.| Empty | Auth |
| controller.auth.tokenSigningKey | Signing key used to sign the delegation token passed on to the segment store. | Empty | Auth |


## Security Configurations in Standalone Mode

For ease of use, Pravega standalone hides some of the configurations that are mentioned above for configuring
Segment Store and Controller services individually for deployments in distributed mode.

|Parameter|Details|Default Value|Feature|
|---------|-------|-------------|-------|
|singlenode.enableTls | Whether to enable TLS for client-server communications. | False | TLS |
|singlenode.certFile | Path of the Certificate file containing the public key, to be used for TLS. |Empty| TLS |
|singlenode.keyFile | Path of the file containing the private key, to be used for TLS. | Empty | TLS |
|singlenode.keyStoreJKS | Path of the keystore file in `.jks` format, to be used for REST API. | Empty | TLS |
|singlenode.keyStoreJKSPasswordFile | Path of the file containing the REST API's keystore password. | Empty | TLS |
|singlenode.trustStoreJKS | Path of the truststore file, to be used for communicating with segment stores. | Empty | TLS |
|singlenode.enableAuth | Whether to enable authentication and authorization for clients. |False| Auth |
|singlenode.passwdFile | Path of the file containing user credentials and ACLs, to be used by the  PasswordAuthHandler.|Empty| Auth |
|singlenode.userName | The default username used for internal communication between segment store and controller| Empty| Auth |
|singlenode.passwd | The default password used for internal communication between segment store and controller| Empty| Auth |
