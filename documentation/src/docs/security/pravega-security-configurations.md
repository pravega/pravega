<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Security Configurations

The following is the list of configuration parameters for different Pravega components.

## Pravega Segment store <a name = "pravega-segment-store"></a>

|Parameter|Details|Default Value|
|---------|-------|-------------|
|pravegaservice.enableTls| Enable TLS on client to segment store connection.|False|
|pravegaservice.certFile|Certificate file used for TLS (public key)| Empty|
|pravegaservice.keyFile|Key file (Private key) used for TLS|Empty|
|autoScale.tlsEnabled| Enable TLS for internal communication between segment store and controller|False|
|autoScale.authEnabled|Enable authorization/authentication for internal communication  between segment store and controller|False|
|autoScale.tlsCertFile|Certificate file used for encrypted internal communication between segment store and controller| Empty|
|autoScale.tokenSigningKey|Signing key used to sign the delegation token sent from controller to segment store| Empty|
|bookkeeper.tlsEnabled|Enable TLS for communication between segment store and Apache Bookkeeper| False|
|bookkeeper.tlsTrustStorePath| Truststore for TLS communication between segment store and Apache Bookkeeper| Empty |

## Pravega Controller <a name ="pravega-controller"></a>

|Parameter|Details|Default Value|
|---------|-------|-------------|
|config.controller.server.authorizationEnabled|Enable authorization/authentication| False|
|config.controller.server.tlsEnabled|Enable encrypted channel between Pravega client and controller|False|
|config.controller.server.tlsKeyFile|The key file (Private key) for communication between Pravega client and controller|Empty|
|config.controller.server.tlsCertFile|Public key certificate for communication between Pravega client and controller|Empty|
|config.controller.server.tokenSigningKey|Signing key used to sign the delegation token passed on to the segment store|Empty|
|config.controller.server.userPasswordFile|File containing user details for default _auth_ implementation for Pravega (similar to `/etc/passwd`)|Empty|

## Pravega Standalone
For ease of use Pravega standalone hides some of the configurations that are mentioned above. Below is the table containing relevant configurations for Pravega Standalone:

|Parameter|Details|Default Value|
|---------|-------|-------------|
|singlenode.enableTls|Enable TLS between all the components deployed within the singlenode| False|
|singlenode.enableAuth|Enable authentication/authorization between all the components within the singlenode |False|
|singlenode.certFile|If TLS is enabled, the public key certificate is used for internal communication between segment store and controller|Empty|
|singlenode.keyFile|If TLS is enabled, this represents the private key by all the server sockets| Empty|
|singlenode.passwdFile|If _auth_ is enabled, this represents the password file for the default _auth_ plugin implementation|Empty|
|singlenode.userName|If _auth_ is enabled, this represents the username used for internal communication between segment store and controller|Empty|
|singlenode.passwd|If _auth_ is enabled, this represents the password used for internal communication between segment store and controller|Empty|
