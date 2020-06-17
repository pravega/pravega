<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Security Configurations

This document describes the security configuration parameters of Pravega, in both **distributed** and **standalone** modes.

## Security Configuration Parameters in Distributed Mode

In the distributed mode, Controllers and Segment Stores are configured via separate sets of parameters.

These parameters may be specified via configuration files or Java system properties. Alternatively, you may use environment variables to configure them.

The following sub-sections describe
their Transport Layer Security (TLS) and auth (short for authentication and authorization) parameters.

### Controller

|Parameter (Corresponding Environment Variable) |Details|Default Value|Feature|
|-----------|-------|-------------|-------|
|`controller.auth.tlsEnabled` (`TLS_ENABLED`)| Whether to enable TLS for client-server communication. | False | TLS |
|`controller.auth.segmentStoreTlsEnabled` (`TLS_ENABLED_FOR_SEGMENT_STORE`)| Whether to enable TLS for communications with Segment Store, even if TLS is disabled for the Controller. This is only useful in cases where the Controller has TLS disabled, but the Segment Store has it enabled. | False | TLS |
|`controller.auth.tlsCertFile` (`TLS_CERT_FILE`)| Path of the X.509 PEM-encoded server certificate file for the service. | Empty | TLS |
|`controller.auth.tlsKeyFile` (`TLS_KEY_FILE`)| Path of the PEM-encoded private key file for the service. | Empty | TLS |
|`controller.auth.tlsTrustStore` (`TLS_TRUST_STORE`)| Path of the PEM-encoded truststore file for TLS connections with Segment Stores. | Empty | TLS |
|`controller.rest.tlsKeyStoreFile` (`REST_KEYSTORE_FILE_PATH`)| Path of the keystore file in `.jks` for the REST interface. | Empty | TLS |
|`controller.rest.tlsKeyStorePasswordFile` (`REST_KEYSTORE_PASSWORD_FILE_PATH`) | Path of the file containing the keystore password for the REST interface. | Empty | TLS |
|`controller.zk.secureConnection` (`SECURE_ZK`)| Whether to enable TLS for communication with Apache Zookeeper| False | TLS |
|`controller.zk.tlsTrustStoreFile` (`ZK_TRUSTSTORE_FILE_PATH`)| Path of the truststore file in `.jks` format for TLS connections with Apache Zookeeer. | Empty | TLS |
|`controller.zk.tlsTrustStorePasswordFile` (`ZK_TRUSTSTORE_PASSWORD_FILE_PATH`)| Path of the file containing the password of the truststore used for TLS connections with Apache Zookeeper. | Empty | TLS |
|`controller.auth.enabled` (`AUTHORIZATION_ENABLED`)| Whether to enable authentication and authorization for clients. | False | Auth |
|`controller.auth.userPasswordFile` (`USER_PASSWORD_FILE`)| Path of the file containing user credentials and ACLs, for the PasswordAuthHandler.| Empty | Auth |
|`controller.auth.tokenSigningKey` (`TOKEN_SIGNING_KEY`)| Key used to sign the delegation tokens for Segment Stores. | Empty | Auth |

### Segment Store

|Parameter (Corresponding Environment Variable|Description|Default Value|Feature|
|----------------------|-------------|-------------|------------|
|`pravegaservice.enableTls` (`ENABLE_TLS`)| Whether to enable TLS for client-server communications. | False | TLS |
|`pravegaservice.enableTlsReload` (`ENABLE_TLS_RELOAD`)| Whether to automatically reload SSL/TLS context if the server certificate is updated. | False | TLS |
|`pravegaservice.certFile` (`CERT_FILE`)| Path of the X.509 PEM-encoded server certificate file for the service. | Empty | TLS |
|`pravegaservice.keyFile` (`KEY_FILE`)| Path of the PEM-encoded private key file for the service. | Empty | TLS |
|`pravegaservice.secureZK` (`SECURE_ZK`)| Whether to enable TLS for communication with Apache Zookeeper. | False | TLS |
|`pravegaservice.zkTrustStore` (`ZK_TRUSTSTORE_LOCATION`)| Path of the truststore file in `.jks` format for TLS connections with Apache Zookeeer. | Empty | TLS |
|`pravegaservice.zkTrustStorePasswordPath` (`ZK_TRUST_STORE_PASSWORD_PATH`)| Path of the file containing the password of the truststore used for TLS connections with Apache Zookeeper. | Empty | TLS |
|`autoScale.tlsEnabled` (`TLS_ENABLED`)| Whether to enable TLS for internal communication with the Controllers. | False | TLS |
|`autoScale.tlsCertFile` (`TLS_CERT_FILE`)| Path of the PEM-encoded X.509 certificate file used for TLS connections with the Controllers. | Empty | TLS |
|`autoScale.validateHostName` (`VALIDATE_HOSTNAME`)| Whether to enable hostname verification for TLS connections with the Controllers. | True | TLS |
|`autoScale.authEnabled` (`AUTH_ENABLED`)| Whether to enable authentication and authorization for internal communications with the Controllers. | False | Auth |
|`autoScale.tokenSigningKey` (`TOKEN_SIGNING_KEY`)| The key used for signing the delegation tokens. | Empty | Auth |
|`bookkeeper.tlsEnabled` (`BK_TLS_ENABLED`)| Whether to enable TLS for communication with Apache Bookkeeper. | False | TLS |
|`bookkeeper.tlsTrustStorePath` (`TLS_TRUST_STORE_PATH`)| Path of the truststore file in `.jks` format for TLS connections with Apache Bookkeeper. | Empty | TLS |
|`pravega.client.auth.loadDynamic` (`pravega_client_auth_loadDynamic`)| Whether to load a credentials object dynamically from a class available in Classpath.  | false | Auth |
|`pravega.client.auth.token` (`pravega_client_auth_method`)| The token to use by the Auto Scale Processor when communicating with Controller.  | Empty | Auth |
|`pravega.client.auth.method` (`pravega_client_auth_token`)| The `auth` method to use by the Auto Scale Processor when communicating with Controller. | Empty | Auth |


## Security Configurations in Standalone Mode

For ease of use, Pravega standalone mode abstracts away some of the configuration parameters of distributed mode. As a result, it has
fewer security configuration parameters to configure.


|Parameter|Details|Default Value|Feature|
|---------|-------|-------------|-------|
| `singlenode.enableTls` | Whether to enable TLS for client-server communications. | False | TLS |
| `singlenode.certFile` | Path of the X.509 PEM-encoded server certificate file for the server. |Empty| TLS |
| `singlenode.keyFile` | Path of the PEM-encoded private key file for the service. | Empty | TLS |
| `singlenode.keyStoreJKS` | Path of the keystore file in `.jks` for the REST interface. | Empty | TLS |
| `singlenode.keyStoreJKSPasswordFile` |Path of the file containing the keystore password for the REST interface. | Empty | TLS |
| `singlenode.trustStoreJKS` | Path of the truststore file for internal TLS connections. | Empty | TLS |
| `singlenode.enableAuth` | Whether to enable authentication and authorization for clients. |False| Auth |
| `singlenode.passwdFile` | Path of the file containing user credentials and ACLs, for the PasswordAuthHandler. |Empty| Auth |
| `singlenode.userName` | The default username used for internal communication between Segment Store and Controller. | Empty| Auth |
| `singlenode.passwd` | The default password used for internal communication between Segment Store and Controller. | Empty| Auth |
| `singlenode.segmentstoreEnableTlsReload` | Whether to automatically reload SSL/TLS context if the server certificate is updated. | False | TLS |
