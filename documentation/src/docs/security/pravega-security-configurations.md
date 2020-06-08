<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

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

|Parameter  |Details|Default Value|Feature|
|-----------|-------|-------------|-------|
|`controller.security.tls.enable` | Whether to enable TLS for client-server communication. | False | TLS |
|`controller.security.tls.server.certificate.location` | Path of the X.509 PEM-encoded server certificate file for the service. | Empty | TLS |
|`controller.security.tls.server.privateKey.location` | Path of the PEM-encoded private key file for the service. | Empty | TLS |
|`controller.security.tls.server.keyStore.location` | Path of the keystore file in `.jks` for the REST interface. | Empty | TLS |
|`controller.security.tls.server.keyStore.pwd.location` | Path of the file containing the keystore password for the REST interface. | Empty | TLS |
|`controller.security.tls.trustStore.location` | Path of the PEM-encoded truststore file for TLS connections with Segment Stores. | Empty | TLS |
|`controller.segmentstore.connect.channel.tls` | Whether to enable TLS for communications with Segment Store, even if TLS is disabled for the Controller. This is only useful in cases where the Controller has TLS disabled, but the Segment Store has it enabled. | False | TLS |
|`controller.security.auth.enable` | Whether to enable authentication and authorization for clients. | False | Auth |
|`controller.security.auth.delegationToken.signingKey.basis` | Key used to sign the delegation tokens for Segment Stores. | Empty | Auth |
|`controller.security.pwdAuthHandler.accountsDb.location` | Path of the file containing user credentials and ACLs, for the PasswordAuthHandler.| Empty | Auth |
|`controller.zk.connect.security.enable` | Whether to enable TLS for communication with Apache Zookeeper| False | TLS |
|`controller.zk.connect.security.tls.trustStore.location` | Path of the truststore file in `.jks` format for TLS connections with Apache Zookeeer. | Empty | TLS |
|`controller.zk.connect.security.tls.trustStore.pwd.location` | Path of the file containing the password of the truststore used for TLS connections with Apache Zookeeper. | Empty | TLS |

### Segment Store

|Parameter (Corresponding Environment Variable) |Description|Default Value|Feature|
|----------------------|-------------|-------------|------------|
|`pravegaservice.security.tls.enable` | Whether to enable TLS for client-server communications. | False | TLS |
|`pravegaservice.security.tls.certificate.autoReload.enable` | Whether to automatically reload SSL/TLS context if the server certificate is updated. | False | TLS |
|`pravegaservice.security.tls.server.certificate.location` | Path of the X.509 PEM-encoded server certificate file for the service. | Empty | TLS |
|`pravegaservice.security.tls.server.privateKey.location` | Path of the PEM-encoded private key file for the service. | Empty | TLS |
|`pravegaservice.zk.connect.security.enable` | Whether to enable TLS for communication with Apache Zookeeper. | False | TLS |
|`pravegaservice.zk.connect.security.tls.trustStore.location` | Path of the truststore file in `.jks` format for TLS connections with Apache Zookeeer. | Empty | TLS |
|`pravegaservice.zk.connect.security.tls.trustStore.pwd.location` | Path of the file containing the password of the truststore used for TLS connections with Apache Zookeeper. | Empty | TLS |
|`autoScale.controller.connect.security.tls.enable` | Whether to enable TLS for internal communication with the Controllers. | False | TLS |
|`autoScale.controller.connect.security.tls.truststore.location` | Path of the PEM-encoded X.509 certificate file used for TLS connections with the Controllers. | Empty | TLS |
|`autoScale.controller.connect.security.tls.validateHostName.enable` | Whether to enable hostname verification for TLS connections with the Controllers. | True | TLS |
|`autoScale.controller.connect.security.auth.enable` | Whether to enable authentication and authorization for internal communications with the Controllers. | False | Auth |
|`autoScale.security.auth.token.signingKey.basis` | The key used for signing the delegation tokens. | Empty | Auth |
|`bookkeeper.connect.security.tls.enable` | Whether to enable TLS for communication with Apache Bookkeeper. | False | TLS |
|`bookkeeper.connect.security.tls.trustStore.location` | Path of the truststore file in `.jks` format for TLS connections with Apache Bookkeeper. | Empty | TLS |
|`pravega.client.auth.loadDynamic` (`pravega_client_auth_loadDynamic`)| Whether to load a credentials object dynamically from a class available in Classpath.  | false | Auth |
|`pravega.client.auth.token` (`pravega_client_auth_method`)| The token to use by the Auto Scale Processor when communicating with Controller.  | Empty | Auth |
|`pravega.client.auth.method` (`pravega_client_auth_token`)| The `auth` method to use by the Auto Scale Processor when communicating with Controller. | Empty | Auth |


## Security Configurations in Standalone Mode

For ease of use, Pravega standalone mode abstracts away some of the configuration parameters of distributed mode. As a result, it has
fewer security configuration parameters to configure.


|Parameter|Details|Default Value|Feature|
|---------|-------|-------------|-------|
| `singlenode.security.tls.enable` | Whether to enable TLS for client-server communications. | False | TLS |
| `singlenode.security.tls.certificate.location` | Path of the X.509 PEM-encoded server certificate file for the server. |Empty| TLS |
| `singlenode.security.tls.privateKey.location` | Path of the PEM-encoded private key file for the service. | Empty | TLS |
| `singlenode.security.tls.keyStore.location` | Path of the keystore file in `.jks` for the REST interface. | Empty | TLS |
| `singlenode.security.tls.keyStore.pwd.location` |Path of the file containing the keystore password for the REST interface. | Empty | TLS |
| `singlenode.security.tls.trustStore.location` | Path of the truststore file for internal TLS connections. | Empty | TLS |
| `singlenode.security.auth.enable` | Whether to enable authentication and authorization for clients. |False| Auth |
| `singlenode.security.auth.credentials.username` | The default username used for internal communication between Segment Store and Controller. | Empty| Auth |
| `singlenode.security.auth.credentials.pwd` | The default password used for internal communication between Segment Store and Controller. | Empty| Auth |
| `singlenode.security.auth.pwdAuthHandler.accountsDb.location` | Path of the file containing user credentials and ACLs, for the PasswordAuthHandler. |Empty| Auth |
| `singlenode.segmentStore.tls.certificate.autoReload.enable` | Whether to automatically reload SSL/TLS context if the server certificate is updated. | False | TLS |
