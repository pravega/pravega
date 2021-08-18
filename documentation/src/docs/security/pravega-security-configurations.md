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
# Pravega Security Configurations

This document describes the security configuration parameters of Pravega, in both **distributed** and **standalone** modes.

**Table of Contents:**

  * [Security Configuration Parameters in Distributed Mode](#security-configuration-parameters-in-distributed-mode)
    + [Controller TLS Configuration Parameters](#controller-tls-configuration-parameters)
    + [Controller Authentication and Authorization Configuration Parameters](#controller-authentication-and-authorization-configuration-parameters)
    + [Segment Store TLS Configuration Parameters](#segment-store-tls-configuration-parameters)
    + [Segment Store Authentication and Authorization Configuration Parameters](#segment-store-authentication-and-authorization-configuration-parameters)
  * [Security Configurations in Standalone Mode](#security-configurations-in-standalone-mode)

## Security Configuration Parameters in Distributed Mode

In the distributed mode, Controllers and Segment Stores are configured via separate sets of parameters.

These parameters may be specified via configuration files or Java system properties. Alternatively, you may use environment variables to configure them.

The following sub-sections describe
their Transport Layer Security (TLS) and auth (short for authentication and authorization) parameters.

### Controller TLS Configuration Parameters

* __<ins>controller.security.tls.enable</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Whether to enable TLS for client-server communication. |
   |Type:|boolean|
   |Default:| `false`|
   |Valid values:|{`true`, `false`} |
   |Old name:|`controller.auth.tlsEnabled` (deprecated) |
    
* __<ins>controller.security.tls.protocolVersion</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Configurable versions for TLS Protocol |
   |Type:|String|
   |Default:| `TLSv1.2,TLSv1.3`|
   |Valid values:|{`TLSv1.2`, `TLSv1.3`, `TLSv1.2,TLSv1.3`, `TLSv1.3,TLSv1.2`} |


* __<ins>controller.security.tls.server.certificate.location</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Path of the PEM-encoded file that contains a TLS certificate to use for securing the server's gRPC interface. The TLS certificate contains the public key of the server. |
   |Type:|string|
   | Default:| None |
   |Sample value:|`/path/to/server/server1-cert.crt`   |
   |Old name:|`controller.auth.tlsCertFile` (deprecated) |
     
* __<ins>controller.security.tls.server.privateKey.location</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Path of the PEM-encoded file that contains the private key associated with the server's public key bound in its TLS certificate. This file must be kept private and secured to avoid compromise of TLS security. |
   |Type:|string|
   | Default:| None | 
   |Sample value:|`/path/to/server/server1-privateKey.key`   |
   |Old name:|`controller.auth.tlsKeyFile` (deprecated) |
  
* __<ins>controller.security.tls.server.keyStore.location</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Path of the `.jks` file that contains the TLS material used for securing the Controller's REST interface. It contains the server's public key certificate and the associated pivate key, as well as the CA's certificate.   |
   |Type:|string|
   |Default:| None | 
   |Sample value:|`/path/to/server/server1-keystore.jks`   |
   |Old name:|`controller.rest.tlsKeyStoreFile` (deprecated) |
   
* __<ins>controller.security.tls.server.keyStore.pwd.location</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Path of the file containing the password for the keystore specified via `controller.security.tls.server.keyStore.location`. |
   |Type:|string|
   |Default:| None | 
   |Sample value:|`/path/to/server/server1-keystore.pwd`   |
   |Old name:|`controller.rest.tlsKeyStorePasswordFile` (deprecated) |

* __<ins>controller.security.tls.trustStore.location</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Path of the PEM-encoded file that contains the certificates that the server should trust, when connecting to other services like Segment Store and instances. Typically, it contains the public key certificate of the CA that has signed the services' certificates. It may alternatively contain the service's certificates directly. |
   |Type:|string|
   |Default:| None | 
   |Sample value:|`/path/to/client/truststore.crt`    |
   |Old name:|`controller.auth.tlsTrustStore` (deprecated) |
 
* __<ins>controller.segmentstore.connect.channel.tls</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Whether to enable TLS for communications with Segment Store, even if TLS is disabled for the Controller. This is useful in cases where the Controller has TLS disabled, but the Segment Store has it enabled.  |
   |Type:|string|
   |Default:| Same as that of `controller.security.tls.enable`   | 
   |Valid values:|{`true`, `false`, ``} |
   |Old name:|`controller.auth.segmentStoreTlsEnabled` (deprecated) |
  
* __<ins>controller.zk.connect.security.enable</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: |  Whether to enable security for communications with Apache Zookeeper. |
   |Type:|boolean|
   |Default:| `false`|
   |Valid values:|{`true`, `false`} |
   |Old name:|`controller.zk.secureConnection` (deprecated) |
  
* __<ins>controller.zk.connect.security.tls.trustStore.location</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Path of the truststore file in `.jks` format for TLS connections with Apache Zookeeer.  |
   |Type:|string|
   |Default: | None |
   |Sample value:|`/path/to/client/zookeeper.truststore.crt`  |
   |Old name:|`controller.zk.tlsTrustStoreFile` (deprecated) |   
   
* __<ins>controller.zk.connect.security.tls.trustStore.pwd.location</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Path of the file containing the password of the truststore specified via `controller.zk.connect.security.tls.trustStore.location`. |
   |Type:|string|
   |Default: | None |
   |Sample value:|`/path/to/client/zookeeper.truststore.pwd`  |
   |Old name:|`controller.zk.tlsTrustStorePasswordFile` (deprecated) |   
 
   
### Controller Authentication and Authorization Configuration Parameters
 
* __<ins>controller.security.auth.enable</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Whether to enable authentication and authorization (Auth) for clients. |
   |Type:|boolean|
   |Default:| `false`|
   |Valid values:|{`true`, `false`} |
   |Old name:|`controller.auth.enabled` (deprecated) |
 
* __<ins>controller.security.auth.delegationToken.signingKey.basis</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | String used to generate the key used for signing delegation tokens.  |
   |Type:|string|
   |Default: | None |
   |Sample value:|`super-secret-key` |
   |Old name:|`controller.auth.tokenSigningKey` (deprecated) |   
 
* __<ins>controller.security.pwdAuthHandler.accountsDb.location</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Path of the file containing a listing of user accounts and their permissions. This file is used by the Password Auth Handler (the built-in Auth Handler implementation). |
   |Type:|string|
   |Default: | None |
   |Sample value:|``/path/to/accountsDB` |
   |Old name:|``controller.auth.userPasswordFile` (deprecated) | 

### Segment Store TLS Configuration Parameters

* __<ins>pravegaservice.security.tls.enable</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: |  Whether to enable TLS for client-server communication. |
   |Type:|boolean|
   |Default:| `false`|
   |Valid values:|{`true`, `false`} |
   |Old name:|`pravegaservice.enableTls` (deprecated) |

* __<ins>pravegaservice.security.tls.protocolVersion</ins>__
   
   |Property| Value |
   |---:|:----|
   |Description: | Configurable versions for TLS Protocol |
  |Type:|String|
  |Default:| `TLSv1.2,TLSv1.3`|
  |Valid values:|{`TLSv1.2`, `TLSv1.3`, `TLSv1.2,TLSv1.3`, `TLSv1.3,TLSv1.2`} |
   
  
* __<ins>pravegaservice.security.tls.certificate.autoReload.enable</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Whether to automatically reload SSL/TLS context if the server certificate file is updated. |
   |Type:|boolean|
   |Default:| `false`|
   |Valid values:|{`true`, `false`} |
   |Old name:|`pravegaservice.enableTlsReload` (deprecated) |
 
* __<ins>pravegaservice.security.tls.server.certificate.location</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Path of the PEM-encoded file that contains a TLS certificate to use for securing the server's interface. The TLS certificate contains the public key of the server.  |
   |Type:|string|
   |Default: | None |
   |Sample value:|`/path/to/server/server-cert.crt` |
   |Old name:|`pravegaservice.certFile` (deprecated) | 
   
* __<ins>pravegaservice.security.tls.server.privateKey.location</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Path of the PEM-encoded file that contains the private key associated with the server's public key bound in its TLS certificate. This file must be kept private and secured to avoid compromise of TLS security. |
   |Type:|string|
   |Default: | None |
   |Sample value:| `/path/to/server/server-privateKey.key`  |
   |Old name:|`pravegaservice.keyFile` (deprecated) | 

* __<ins>pravegaservice.security.tls.server.keyStore.location</ins>__

  |Property| Value | 
     |---:|:----|
  |Description: | Path of the `.jks` file that contains the TLS material used for securing the Segment Store's REST interface. It contains the server's public key certificate and the associated pivate key, as well as the CA's certificate.   |
  |Type:|string|
  |Default:| None | 
  |Sample value:|`/path/to/server/server-keystore.jks`   |

* __<ins>pravegaservice.security.tls.server.keyStore.pwd.location</ins>__

  |Property| Value | 
     |---:|:----|
  |Description: | Path of the file containing the password for the keystore specified via `pravegaservice.security.tls.server.keyStore.location`. |
  |Type:|string|
  |Default:| None | 
  |Sample value:|`/path/to/server/server-keystore.pwd`   |
  
* __<ins>autoScale.controller.connect.security.tls.enable</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Whether to enable TLS for internal communication with the Controllers. |
   |Type:|boolean|
   |Default:| `false`|
   |Valid values:|{`true`, `false`} |
   |Old name:|`autoScale.tlsEnabled` (deprecated) |

* __<ins>autoScale.controller.connect.security.tls.truststore.location</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Path of the PEM-encoded file that contains the certificates that the server should trust, when connecting to other services like Controller and other instances. Typically, it contains the public key certificate of the CA that has signed the services' certificates. It may alternatively contain the service's certificates directly. |
   |Type:|string|
   |Default: | None |
   |Sample value:| `/path/to/client/truststore.crt`  |
   |Old name:|`autoScale.tlsCertFile` (deprecated) | 

* __<ins>autoScale.controller.connect.security.tls.validateHostName.enable</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Whether to enable hostname verification for TLS connections with the Controllers. |
   |Type:|boolean|
   |Default:| `false`|
   |Valid values:|{`true`, `false`} |
   |Old name:|`autoScale.validateHostName` (deprecated) |
 
* __<ins>pravegaservice.zk.connect.security.enable</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Whether to enable security for communications with Apache Zookeeper instances. |
   |Type:|boolean|
   |Default:| `false`|
   |Valid values:|{`true`, `false`} |
   |Old name:|`pravegaservice.secureZK` (deprecated) |


* __<ins>pravegaservice.zk.connect.security.tls.trustStore.location</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Path of the truststore file in `.jks` format for TLS connections with Apache Zookeeer instances. |
   |Type:|string|
   |Default: | None |
   |Sample value:| `/path/to/client/zookeeper.truststore.crt`  |
   |Old name:|`pravegaservice.zkTrustStore` (deprecated) | 
   
* __<ins>pravegaservice.zk.connect.security.tls.trustStore.pwd.location</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Path of the file containing the password of the truststore specified via `pravegaservice.zk.connect.security.tls.trustStore.location`. |
   |Type:|string|
   |Default: | None |
   |Sample value:| `/path/to/client/zookeeper.truststore.pwd`   |
   |Old name:|`pravegaservice.zkTrustStorePasswordPath` (deprecated) | 
  
* __<ins>pravegaservice.bookkeeper.connect.security.enable</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Whether to enable security for communications with Apache Bookkeeper instances. |
   |Type:|boolean|
   |Default:| `false`|
   |Valid values:|{`true`, `false`} |
   |Old name:|`bookkeeper.tlsEnabled` (deprecated) |

* __<ins>bookkeeper.connect.security.tls.trustStore.location</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Path of the truststore file in `.jks` format for TLS connections with Apache Bookkeeper instances. |
   |Type:|string|
   |Default: | None |
   |Sample value:| `/path/to/client/zookeeper.truststore.crt`   |
   |Old name:|`bookkeeper.tlsTrustStorePath` (deprecated) | 
    

### Segment Store Authentication and Authorization Configuration Parameters

* __<ins>autoScale.controller.connect.security.auth.enable</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | Whether to enable authentication and authorization (Auth) for internal communications with the Controllers. |
   |Type:|boolean|
   |Default:| `false`|
   |Valid values:|{`true`, `false`} |
   |Old name:|`autoScale.authEnabled` (deprecated) |

* __<ins>autoScale.security.auth.token.signingKey.basis</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | A string used to generate the key used for signing delegation tokens. This value must be the same that set in `controller.security.auth.token.signingKey.basis` for Controllers.  |
   |Type:|string|
   |Default: | `secret`|
   |Sample value:| `super-secret-key`  |
   |Old name:|`autoScale.tokenSigningKey` (deprecated) | 

* __<ins>pravega.client.auth.loadDynamic</ins>__ 

   |Property| Value | 
   |---:|:----|
   |Description: | Whether to load a credentials object dynamically from a class available in Classpath, for the Auto Scale Processor's authentication to the Controller. |
   |Type:|boolean|
   |Default: |{`true`, `false`}|
   |Sample value:| `super-secret-key`  |
   |Alternative method:|`pravega_client_auth_loadDynamic` (environment variable)|
 
* __<ins>pravega.client.auth.method</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | The `auth` method to use by the Auto Scale Processor when communicating with Controller. |
   |Type:|string|
   |Default: | None |
   |Sample value:| `Basic`  |
   |Alternative method:|`pravega_client_auth_token` (environment variable)|
  
* __<ins>pravega.client.auth.token</ins>__

   |Property| Value | 
   |---:|:----|
   |Description: | The token to used by the Auto Scale Processor for its authentication to the Controller. The format of the token depends on the `pravega.client.auth.method`. For `Basic` authentication method, the value is a Base 64 encoded string of the input string `<username>:<password>`.  |
   |Type:|string|
   |Default: | None |
   |Sample value:| `YXV0b1NjYWxlclVzZXIxOnN1cGVyLXNlY3JldC1wYXNzd29yZA==` (Base 64 encoded value of credentials in Basic format 'autoScalerUser1:super-secret-password')    |
   |Alternative method:|`pravega_client_auth_token` (environment variable)|

## Security Configurations in Standalone Mode

For ease of use, Pravega standalone mode abstracts away some of the configuration parameters of distributed mode. As a result, it has
fewer security configuration parameters to configure.


|Parameter|Details|Default |Feature|
|---------|-------|-------------|-------|
| `singlenode.security.tls.enable` | Whether to enable TLS for client-server communications. | `false` | TLS |
| `singlenode.security.tls.protocolVersion` | Version of the TLS Protocol. | `TLSv1.2,TLSv1.3` | TLS|
| `singlenode.security.tls.certificate.location` | Path of the X.509 PEM-encoded server certificate file for the server. | None | TLS |
| `singlenode.security.tls.privateKey.location` | Path of the PEM-encoded private key file for the service. | None | TLS |
| `singlenode.security.tls.keyStore.location` | Path of the keystore file in `.jks` for the REST interface. | None | TLS |
| `singlenode.security.tls.keyStore.pwd.location` |Path of the file containing the keystore password for the REST interface. | None | TLS |
| `singlenode.security.tls.trustStore.location` | Path of the truststore file for internal TLS connections. | None | TLS |
| `singlenode.security.auth.enable` | Whether to enable authentication and authorization for clients. | `false` | Auth |
| `singlenode.security.auth.credentials.username` | The default username used for internal communication between Segment Store and Controller. | None | Auth |
| `singlenode.security.auth.credentials.pwd` | The default password used for internal communication between Segment Store and Controller. | None | Auth |
| `singlenode.security.auth.pwdAuthHandler.accountsDb.location` | Path of the file containing user credentials and ACLs, for the PasswordAuthHandler. | None | Auth |
| `singlenode.segmentStore.tls.certificate.autoReload.enable` | Whether to automatically reload SSL/TLS context if the server certificate is updated. | `false` | TLS |
