<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Security Configurations

The following is the list of configuration parameters for different Pravega components.

## Pravega Segment Store <a name = "pravega-segment-store"></a>

|Parameter|Details|Default Value|
|---------|-------|-------------|
|pravegaservice.enableTls| Enable TLS on client to segment store connection.|False|
|pravegaservice.certFile|Certificate file used for TLS (public key)| Empty|
|pravegaservice.keyFile|Key file (Private key) used for TLS|Empty|
|autoScale.tlsEnabled| Enable TLS for internal communication between Segment Store and Controller|False|
|autoScale.authEnabled|Enable authorization/authentication for internal communication  between Segment Store and Controller|False|
|autoScale.tlsCertFile|Certificate file used for encrypted internal communication between Segment Store and Controller| Empty|
|autoScale.tokenSigningKey|Signing key used to sign the delegation token sent from controller to Segment Store| Empty|
|bookkeeper.tlsEnabled|Enable TLS for communication between Segment Store and Apache Bookkeeper| False|
|bookkeeper.tlsTrustStorePath| Truststore for TLS communication between Segment Store and Apache Bookkeeper| Empty |
|pravegaservice.secureZK|Enable TLS for communication between Segment Store and Apache Zookeeper| False|
|bookkeeper.zkTrustStore| Truststore for TLS communication between Segment Store and Apache Zookeeper| Empty |

## Pravega Controller <a name ="pravega-controller"></a>

|Parameter|Details|Default Value|
|---------|-------|-------------|
|config.controller.server.authorizationEnabled|Enable authorization/authentication| False|
|config.controller.server.tlsEnabled|Enable encrypted channel between Pravega client and Controller|False|
|config.controller.server.tlsKeyFile|The key file (Private key) for communication between Pravega client and Controller|Empty|
|config.controller.server.tlsCertFile|Public key certificate for communication between Pravega client and Controller|Empty|
|config.controller.server.tokenSigningKey|Signing key used to sign the delegation token passed on to the Segment Store|Empty|
|config.controller.server.userPasswordFile|File containing user details for default _auth_ implementation for Pravega (similar to `/etc/passwd`)|Empty|
|config.controller.server.zk.secureConnectionToZooKeeper|Enable TLS for connection to Apache ZooKeeper| False|
|config.controller.server.zk.trustStorePath|Truststore for TLS communications with Apache ZooKeeper| False|

## Pravega Standalone
For ease of use Pravega Standalone hides some of the configurations that are mentioned above. Below is the table containing relevant configurations for Pravega Standalone:

|Parameter|Details|Default Value|
|---------|-------|-------------|
|singlenode.enableTls|Enable TLS between all the components deployed within the singlenode| False|
|singlenode.enableAuth|Enable authentication/authorization between all the components within the singlenode |False|
|singlenode.certFile|If TLS is enabled, the Public key certificate is used for internal communication between Segment Store and Controller|Empty|
|singlenode.keyFile|If TLS is enabled, this represents the Private key by all the server sockets| Empty|
|singlenode.passwdFile|If _auth_ is enabled, this represents the password file for the default _auth_ plugin implementation|Empty|
|singlenode.userName|If _auth_ is enabled, this represents the user name used for internal communication between Segment Store and Controller|Empty|
|singlenode.passwd|If _auth_ is enabled, this represents the password used for internal communication between Segment Store and Controller|Empty|
