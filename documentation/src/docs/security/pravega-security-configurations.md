<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Security Configurations

This document describes the security configuration parameters for Pravega, in both distributed and standalone modes.

## Security Configuration Parameters in Distributed Mode

In the distributed mode, Controllers and Segment Stores are configured individually. The following sub-sections describe
their Transport Layer Security (TLS) and Auth (Authentication and Authorization) parameters.


### Segment store <a name = "pravega-segment-store"></a>

|Parameter|Description|Default Value|Feature|
|---------|-------|-------------|------------|
| pravegaservice.enableTls | Whether to enable TLS for client-server communications. | False | TLS |
| pravegaservice.certFile | Path of the X.509 PEM-encoded server certificate file for the service. | Empty | TLS |
| pravegaservice.keyFile | Path of the PEM-encoded private key file for the service. | Empty | TLS |
| pravegaservice.secureZK | Whether to enable TLS for communication with Apache Zookeeper. | False | TLS |
| pravegaservice.zkTrustStore | Path of the truststore file in `.jks` format for TLS connections with Apache Zookeeer. | Empty | TLS |
| pravegaservice.zkTrustStorePasswordPath | Path of the file containing the password of the truststore used for TLS connections with Apache Zookeeper. | Empty | TLS |
| autoScale.tlsEnabled | Whether to enable TLS for internal communication with the Controllers. | False | TLS |
| autoScale.tlsCertFile | Path of the PEM-encoded X.509 certificate file used for TLS connections with the Controllers. | Empty | TLS |
| autoScale.validateHostName | Whether to enable hostname verification for TLS connections with the Controllers. | True | TLS |
| autoScale.authEnabled | Whether to enable authentication and authorization for internal communications with the Controllers. | False | Auth |
| autoScale.tokenSigningKey | The key used for signing the delegation tokens. | Empty | Auth |
| bookkeeper.tlsEnabled | Whether to enable TLS for communication with Apache Bookkeeper. | False | TLS |
| bookkeeper.tlsTrustStorePath | Path of the truststore file in `.jks` format for TLS connections with Apache Bookkeeper. | Empty | TLS |


### Controller <a name ="pravega-controller"></a>

|Parameter|Details|Default Value|Feature|
|---------|-------|-------------|-------|
| controller.auth.tlsEnabled | Whether to enable TLS for client-server communication. | False | TLS |
| controller.auth.tlsCertFile | Path of the X.509 PEM-encoded server certificate file for the service. | Empty | TLS |
| controller.auth.tlsKeyFile | Path of the PEM-encoded private key file for the service. | Empty | TLS |
| controller.auth.tlsTrustStore | Path of the PEM-encoded truststore file for TLS connections with Segment Stores. | Empty | TLS |
| controller.rest.tlsKeyStoreFile | Path of the keystore file in `.jks` for the REST interface. | Empty | TLS |
| controller.rest.tlsKeyStorePasswordFile | Path of the file containing the keystore password for the REST interface. | Empty | TLS |
| controller.zk.secureConnection | Whether to enable TLS for communication with Apache Zookeeper| False | TLS |
| controller.zk.tlsTrustStoreFile | Path of the truststore file in `.jks` format for TLS connections with Apache Zookeeer. | Empty | TLS |
| controller.zk.tlsTrustStorePasswordFile | Path of the file containing the password of the truststore used for TLS connections with Apache Zookeeper. | Empty | TLS |
| controller.auth.enabled | Whether to enable authentication and authorization for clients. | False | Auth |
| controller.auth.userPasswordFile | Path of the file containing user credentials and ACLs, for the PasswordAuthHandler.| Empty | Auth |
| controller.auth.tokenSigningKey | Key used to sign the delegation tokens for Segment Stores. | Empty | Auth |


## Security Configurations in Standalone Mode

For ease of use, Pravega standalone hides some of the configurations that are mentioned above for configuring
Segment Store and Controller services individually for deployments in distributed mode.

|Parameter|Details|Default Value|Feature|
|---------|-------|-------------|-------|
|singlenode.enableTls | Whether to enable TLS for client-server communications. | False | TLS |
|singlenode.certFile | Path of the X.509 PEM-encoded server certificate file for the server. |Empty| TLS |
|singlenode.keyFile | Path of the PEM-encoded private key file for the service. | Empty | TLS |
|singlenode.keyStoreJKS | Path of the keystore file in `.jks` for the REST interface. | Empty | TLS |
|singlenode.keyStoreJKSPasswordFile |Path of the file containing the keystore password for the REST interface. | Empty | TLS |
|singlenode.trustStoreJKS | Path of the truststore file for internal TLS connections. | Empty | TLS |
|singlenode.enableAuth | Whether to enable authentication and authorization for clients. |False| Auth |
|singlenode.passwdFile | Path of the file containing user credentials and ACLs, for the PasswordAuthHandler. |Empty| Auth |
|singlenode.userName | The default username used for internal communication between Segment Store and Controller. | Empty| Auth |
|singlenode.passwd | The default password used for internal communication between Segment Store and Controller. | Empty| Auth |
