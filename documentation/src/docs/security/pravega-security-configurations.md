<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
#Pravega Security Configurations

Here is a list of configuration parameters for different Pravega components.

##Pravega Segmentstore

|Parameter|Details|Default value|
|---------|-------|-------------|
|pravegaservice.enableTls| Enable TLS on client to segmentstore connection.|false|
|pravegaservice.certFile|Certificate file used for TLS (public key)| Empty|
|pravegaservice.keyFile|Key file (Private key) used for TLS|empty|
|autoScale.tlsEnabled| Enable TLS for internal communication between segmentstore and controller|false|
|autoScale.authEnabled|Enable authorization/authentication for internal communication  between segmentstore and controller|false|
|autoScale.tlsCertFile|Certificate file used for encrypted internal communication between segmentstore and controller| empty|
|autoScale.tokenSigningKey|Signing key used to sign the delegation token sent from controller to segmentstore| Empty|
|bookkeeper.tlsEnabled|Enable TLS for communication between segmentstore and Apache Bookkeeper| false|
|bookkeeper.tlsTrustStorePath| Truststore for TLS communication between segmentstore and Apache Bookkeeper| Empty |

##Pravega Controller

|Parameter|Details|Default value|
|---------|-------|-------------|
|config.controller.server.authorizationEnabled|Enable authorization/authentication| false|
|config.controller.server.tlsEnabled|Enable encrypted channel between Pravega client and controller|false|
|config.controller.server.tlsKeyFile|The key file(Private key) for communication between Pravega client and controller|empty|
|config.controller.server.tlsCertFile|Public key certificate for communication between Pravega client and controller|empty|
|config.controller.server.tokenSigningKey|Signing key used to sign the delegation token passed on to the segmentstore|empty|
|config.controller.server.userPasswordFile|File containing user details for default auth implementation for Pravega (similar to /etc/passwd)|empty|
   
##Pravega Standalone
For ease of use Pravega standalone hides some of the configurations that are mentioned above. Here are the relevant configurations for Pravega Standalone:

|Parameter|Details|Default value|
|---------|-------|-------------|
|singlenode.enableTls|Enable TLS between all the components deployed within the singlenode| false|
|singlenode.enableAuth|Enable authentication/authorization between all the components withing the singlenode |false|
|singlenode.certFile|If TLS is enabled, the public key certificate that is used for internal communication between segmentstore and controller|empty|
|singlenode.keyFile|If TLS is enabled, this represents the private key by all the server sockets| empty|
|singlenode.passwdFile|If auth is enabled, this represents the password file for the default auth plugin implementation|empty|
|singlenode.userName|If auth is enabled, this represents the username used for internal communication between segmentstore and controller|empty|
|singlenode.passwd|If auth is enabled, this represents the password used for internal communication between segmentstore and controller|empty|
