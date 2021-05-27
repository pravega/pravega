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
# Client _auth_ Interface
Pravega client can access Pravega APIs through `grpc`. Some of the admin APIs can be accessed via `REST` API. 
The Authorization/Authentication API and plugin works for both of these interfaces.

## grpc Client _auth_ Interface
If multiple plugin exists, a client selects its _auth_ handler by setting a `grpc` header with the name `method`. 
This is performed by implementing [Credentials](https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/stream/impl/Credentials.java) interface by passing through the [ClientConfig](https://github.com/pravega/pravega/blob/master/client/src/main/java/io/pravega/client/ClientConfig.java) object to the Pravega client.
The parameters for authentication are passed through custom `grpc` headers. These are extracted through `grpc` interceptors and passed on to the specific _auth_ plugin.
This plugin is identified by the `method` header.

## Dynamic extraction of the _auth_ parameters on the client
Dynamic extraction of parameters is also possible using the system properties or environment variables.
The order of preference is listed below:

1. User explicitly provides a credential object through the API. This results in overriding the other settings.
2. System properties: System properties are defined in the format: `pravega.client.auth.*`
3. Environment variables: Environment variables are defined in the format: `pravega_client_auth_*`
4. In case of option 2 and 3, the caller decides on whether, the class needs to be loaded dynamically by setting the property `pravega.client.auth.loadDynamic` to true.
 
# REST Client _auth_ Interface
The `REST` client in order to access the Pravega API uses the similar approach as mentioned in the above sections. The custom _auth_ parameters are sent as the part of the `Authorization` HTTP header.

The `REST` server implementation on Pravega **Controller** extracts these headers and passes it to the valid _auth_ plugin implementation. Then it resumes, if the authentication and authorization matches the intended access pattern.

