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
# Setting Up Security for a Distributed Mode Cluster

   * [Introduction](#introduction)
   * [Generating Certificates and Other TLS Artifacts](#generating-certificates-and-other-tls-artifacts)
   * [Enabling TLS and Auth in Pravega](#enabling-tls-and-auth-in-pravega)
      + [Configuring TLS and Auth on Server Side](#configuring-tls-and-auth-on-server-side)
      + [Configuring TLS and Credentials on Client Side](#configuring-tls-and-credentials-on-client-side)
          - [Server Hostname Verification](#server-hostname-verification)
      + [Having TLS and Auth Take Effect](#having-tls-and-auth-take-effect)
   * [Conclusion](#conclusion)

## Introduction

In the [distributed mode](../deployment/deployment.md#Pravega Modes) of running a Pravega cluster, each service runs
separately on one or more processes, usually spread across multiple machines. The deployment options of this mode
include:

1. A manual deployment in hardware or virtual machines
2. Containerized deployments of these types:
    * A Kubernetes native application deployed using the Pravega Operator
    * A Docker Compose application deployment
    * A Docker Swarm based distributed deployment

Regardless of the deployment option used, setting up Transport Layer Security (TLS) and Pravega Auth (short for
authentication and authorization) are essential requirements of a secure Pravega deployment. TLS encrypts 
client-server as well as internal communication server components. TLS also enables clients to authenticate
the services running on the server. Auth, on the other hand, enables the services to validate that the users accounts 
used for accessing them have requisite access permissions. Pravega strongly recommends enabling both TLS and Auth in
production.

This document lists steps for enabling TLS and auth for manual deployments. Depending on the deployment option 
used and your environment, you might need to modify the steps and commands to suit your specific needs and policies.

## Generating Certificates and Other TLS Artifacts

At a high-level, setting up TLS in Pravega comprises of two distinct activities:

1. Generating TLS Certificates, private Keys, keystore, truststore and other artifacts, which involves 
   steps that Pravega is oblivious to.
2. Enabling and configuring TLS in Pravega.

[Creating TLS Certificates, Keys and Other TLS Artifacts](./generating-tls-artifacts.md) explains how to perform the 
first activity. The [next](#enabling-tls-and-auth-in-pravega) section explains the second activity. 

## Enabling TLS and Auth in Pravega

The high-level steps for enabling TLS and Auth, are:

1. Configuring TLS and auth on server side
2. Configuring TLS and credentials on client Side
3. Having TLS and auth take effect

These steps are discussed in the following sub-sections.

### Configuring TLS and Auth on Server Side

You can configure the following services for TLS and Auth:

1. Controller
2. Segment Store
3. Zookeeper (optional)
4. Bookkeeper (optional)

For information about enabling TLS for Zookeeper and Bookeeper, refer to
their documentation here:
* [ZooKeeper SSL Guide](https://cwiki.apache.org/confluence/display/ZOOKEEPER/ZooKeeper+SSL+User+Guide)
* [BookKeeper - Encryption and Authentication using TLS](https://bookkeeper.apache.org/docs/security/tls/)

**Controller**

Controller services can be configured in two different ways:

1. By specifying the configuration parameter values directly in the `controller.config.properties` file. For example,

   ```
   controller.security.tls.enable=true
   controller.security.tls.protocolVersion=TLSv1.2,TLSv1.3
   controller.security.tls.server.certificate.location=/etc/secrets/server-cert.crt
   ```

2. By specifying configuration parameters as JVM system properties when starting the Controller service instance. 
   
   ```
   # Example: docker-compose.yml file
   ...
   services:
      ...
      controller:
         environment:
             ...
             JAVA_OPTS:
                -dcontroller.security.tls.enable=true
                -dcontroller.security.tls.server.certificate.location=...
                ...
      ...          
   ```

The following table lists the Controller service's TLS and auth parameters as well as samples values, for quick reference. 
For a detailed description of these parameters, refer to the
[Pravega Security Configurations](https://github.com/pravega/pravega/blob/master/documentation/src/docs/security/pravega-security-configurations.md) document.

 | Configuration Parameter| Example Value |
 |:-----------------------:|:-------------|
 | `controller.security.tls.enable` | `true` |
 | `controller.security.tls.protocolVersion` | `TLSv1.2,TLSv1.3` <sup>1</sup>|
 | `controller.security.tls.server.certificate.location` | `/etc/secrets/server-cert.crt` |
 | `controller.security.tls.server.privateKey.location` | `/etc/secrets/server-key.key` |
 | `controller.security.tls.trustStore.location` | `/etc/secrets/ca-cert.crt` |
 | `controller.security.tls.server.keyStore.location` | `/etc/secrets/server.keystore.jks` |
 | `controller.security.tls.server.keyStore.pwd.location` | `/etc/secrets/server.keystore.jks.password` <sup>2</sup> |
 | `controller.zk.connect.security.enable` | `false` <sup>3</sup> |
 | `controller.zk.connect.security.tls.trustStore.location` | Unspecified <sup>3</sup>|
 | `controller.zk.connect.security.tls.trustStore.pwd.location` | Unspecified <sup>3</sup>|
 | `controller.security.auth.enable` | `true` |
 | `controller.security.pwdAuthHandler.accountsDb.location` <sup>4</sup> | `/etc/secrets/password-auth-handler.database` |
 | `controller.security.auth.delegationToken.signingKey.basis` | `a-secret-value` |

 [1]: `TLSv1.2` and `TLSv1.3` strict modes are also allowed.

 [2]: This and other `.password` files are text files containing the password for the corresponding store.

 [3]: It is assumed here that Zookeeper TLS is disabled. You may enable it and specify the corresponding client-side 
 TLS configuration properties via the `controller.zk.*` properties.

 [4]: This configuration property is required when using the default Password Auth Handler only.

**Segment Store**

Segment store supports security configuration via a properties file (`config.properties`) or JVM system properties. The table 
below lists its TLS and auth parameters and sample values. For a detailed discription of these parameters refer to
[Pravega Security Configurations](https://github.com/pravega/pravega/blob/master/documentation/src/docs/security/pravega-security-configurations.md) document.

 | Configuration Parameter| Example Value |
 |:-----------------------:|:-------------|
 | `pravegaservice.security.tls.enable` | `true` |
 | `pravegaservice.security.tls.protocolVersion` | `TLSv1.2,TLSv1.3` <sup>1</sup> |
 | `pravegaservice.security.tls.server.certificate.location` | `/etc/secrets/server-cert.crt` |
 | `pravegaservice.security.tls.certificate.autoReload.enable` | `false` |
 | `pravegaservice.security.tls.server.privateKey.location` | `/etc/secrets/server-key.key` |
 | `pravegaservice.zk.connect.security.enable` | `false` <sup>2</sup> |
 | `pravegaservice.zk.connect.security.tls.trustStore.location` | Unspecified <sup>2</sup>|
 | `pravegaservice.zk.connect.security.tls.trustStore.pwd.location` | Unspecified <sup>2</sup>|
 | `autoScale.controller.connect.security.tls.enable` | `true` |
 | `autoScale.controller.connect.security.tls.truststore.location` | `/etc/secrets/ca-cert.crt` |
 | `autoScale.controller.connect.security.auth.enable` | `true` |
 | `autoScale.security.auth.token.signingKey.basis` | `a-secret-value` <sup>3</sup>|
 | `autoScale.controller.connect.security.tls.validateHostName.enable` | `true` |
 | `pravega.client.auth.loadDynamic` | `false` |
 | `pravega.client.auth.method` | `Basic` |
 | `pravega.client.auth.token` | Base64-encoded value of 'username:password' string | 

[1]: `TLSv1.2` and `TLSv1.3` strict modes are also allowed.

[2]: The secret value you use here must match the same value used for other Controller and Segment Store services.

[3]: It is assumed here that Zookeeper TLS is disabled. You may enable it and specify the corresponding client-side TLS
configuration properties via these properties.

### Configuring TLS and Credentials on Client Side

After enabling and configuring TLS and auth on the server-side services, it's time for the clients' setup.

Clients can be made to trust the server's certificates signed by custom CA's using one of the following ways:

  1. Configure the client application to use the signing CA's certificate as the truststore. Alternatively, use the 
     servers' certificate as the truststore. 
     
     ```java
     ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI("tls://<dns-name-or-ip>:9090")
                .trustStore("/etc/secrets/ca-cert.crt")
                ...
                .build();
     ```
  2. Install the CA's certificate in the Java system key store. 
  3. Create a custom truststore with the CA's certificate and supply it to the Pravega client application,
     via standard JVM system properties `javax.net.ssl.trustStore` and `javax.net.ssl.trustStorePassword`.

For auth, client-side configuration depends on the `AuthHandler` implementation used. If your server is configured to
use the built-in Password Auth Handler that supports "Basic" authentication, you may supply the credentials as shown below.

  ```java
  ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI("tls://<dns-name-or-ip>:9090")
                .trustStore("/etc/secrets/ca-cert.crt")
                .credentials(new DefaultCredentials("<password>", "<username>"))
                .build();
  ```

#### Server Hostname Verification

For client's server hostname verification to succeed during TLS handshake, the hostname/IP address it uses for accessing the
server must match one of the following in the server's certificate:

* Common Name (`CN`) in the certificate's `Subject` field
* One of the `Subject Alternative Names` (SAN) field entries

Even if the server listens on the loopback address `127.0.0.1` and its certificate is assigned to `localhost`, hostname 
verification will fail if the client attempts to access the server via `127.0.0.1`. For the verification to pass, 
the client must access the server using the hostname assigned on the certificate `localhost` and have a hosts file entry 
that maps `localhost` to `127.0.0.1` (which is usually already there). 

Similarly, if the server certificate is assigned to a non-routable hostname on the network (say, `controller01.pravega.io`),
you might need to add an IP address and DNS/host name mapping in the client's operating system hosts file. 

```
10.243.221.34 controller01.pravega.io
```

If you are reusing preexisting certificate for development/testing on new hosts, you might need to disable hostname 
verification. To do so, call `validateHostName(false)` of the ClientConfig builder, as shown below. Never disable 
hostname verification in production.

  ```java
  ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI("tls://<dns-name-or-ip>:9090")
                .trustStore("/etc/secrets/ca-cert.crt")
                .validateHostName(false)
                .credentials(...)
                .build();
  ```

### Having TLS and Auth Take Effect

Any changes to TLS and auth configuration parameters take effect only when the service starts. So, changing those 
configurations require a restart of the services. The same is true for clients, as well.

## Conclusion

This document explained about how to enable security in a Pravega cluster running in distributed mode. 
Specifically, how to perform the following actions were discussed:

* Generating a CA (if needed)
* Generating server certificates and keys for Pravega services
* Signing the generated certificates using the generated CA
* Enabling and configuring TLS and auth on the server Side
* Setting up the `ClientConfig` on the client side for communicating with a Pravega cluster running with TLS and auth enabled
* Having TLS and auth take effect
