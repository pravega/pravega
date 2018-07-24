<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Client Auth interface
Pravega client can access Pravega APIs through `grpc`. Some of the admin APIs can be accessed via `REST` API. 
The authorization/authentication API and plugin works for both these interfaces.

## grpc client auth interface
In case more than one plugin exists, a client selects its auth handler by setting a grpc header with a name "method". 
This is done by implementing [Credentials](https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/stream/impl/Credentials.java) interface.
 This is passed through the [ClientConfig](https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/ClientConfig.java) object to the Pravega client.
The authentication related parameters are passed through custom grpc headers. These are extracted through grpc interceptors and passed on to the specific auth plugin.
This plugin is identified by the `method` header.

## Dynamic extraction of the auth parameters on the client
The parameters can also be extracted dynamically from the system properties OR environment variables.
Here is the order of preference in descending order

1. User explicitly provides a credential object through the API. This overrides any other settings.
2. System properties: System properties are defined in the format: `pravega.client.auth.*`
3. Environment variables. Environment variables are defined under the format `pravega_client_auth_*`
4. In case of option 2 and 3, the caller can decide whether the class needs to be loaded dynamically by setting property `pravega.client.auth.loadDynamic` to true.
 
# REST client auth interface
The REST client to access Pravea API uses the similar approach. The custom auth parameters are sent as part of the `Authorization` HTTP header.

The REST server implementation on Pravega Controller extracts these headers, passes it to valid auth plugin implementation
and then resumes with the call if the authentication and authorization matches the intended access pattern.

