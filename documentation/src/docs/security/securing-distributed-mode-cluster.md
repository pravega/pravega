<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Setting Up Security for a Distributed Mode Cluster

   * [Introduction](#introduction)
   * [Setting Up SSL/TLS](#setting-up-ssltls)
       - [Stage 1: Setting Up a Certificate Authority (CA)](#stage-1-setting-up-a-certificate-authority-ca)
       - [Stage 2: Obtaining Server Certificates and Keys](#stage-2-obtaining-server-certificates-and-keys)
       - [Stage 3: Enabling TLS and Deploying Certificates](#stage-3-enabling-tls-and-deploying-certificates)
   * [Enabling TLS and Auth in Pravega](#enabling-tls-and-auth-in-pravega)
       - [Configuring TLS and Auth on Server Side](#configuring-tls-and-auth-on-server-side)
       - [Configuring TLS and Credentials on Client Side](#configuring-tls-and-credentials-on-client-side)
            * [Server Hostname Verification](#server-hostname-verification)
       - [Having TLS and Auth Take Effect](#having-tls-and-auth-take-effect)
   * [Conclusion](#conclusion)

## Introduction

In the [distributed mode](../deployment/deployment.md#pravega-modes) of running a Pravega cluster, each service runs
separately on one or more processes, usually spread across multiple machines. The deployment options of this mode
include:

1. A manual deployment in hardware or virtual machines
2. Containerized deployments of these types:
    * A Kubernetes native application deployed using the Pravega Operator
    * A Docker Compose application deployment
    * A Docker Swarm based distributed deployment

Regardless of the deployment option used, setting up Transport Layer Security (SSL/TLS) and Auth (short for
authentication and authorization) are important steps towards a secure Pravega deployment. 

TLS is the modern counterpart of the SSL. This document uses the terms TLS and SSL/TLS interchangeably, henceforth. 
TLS encrypts client-server and internal communications among server components. It also enables clients to authenticate 
the services running on the server nodes. "Auth" enables Pravega services to authenticate and authorize the clients
and each other. Pravega strongly recommends enabling both TLS and Auth for production clusters.

This document shows steps involved in enabling TLS and auth for manual deployments. Depending on the deployment option 
used and your environment, you might need to modify the steps and commands to suit your specific needs and policies.

## Setting Up SSL/TLS

At a high level, setting up TLS can be divided into two distinct stages:

1. Obtaining server certificates and private keys, which involves steps that Pravega is oblivious to
3. Enabling TLS and Deploying certificates in Pravega 

Stage 1 is described in [Creating TLS Certificates and Keys](generating-tls-material.md). Stage 2 is discussed in the 
[next](#enabling-tls-and-auth-in-pravega) section.

## Enabling TLS and Auth in Pravega

Enabling TLS and auth in Pravega involves the following steps:

1. Configuring TLS and auth on server side
2. Configuring TLS and credentials on client Side
3. Having TLS and auth take effect

These steps are discussed in the following sub-sections.

### Configuring TLS and Auth on Server Side

This step is about using the certificates, keys, keystores and truststores generated earlier to configure
TLS and Auth for the services on the server side.

Note that if you enable TLS and Auth on one service, you must enable them for all the other services too. Pravega
does not support using TLS and Auth for only some instances of the services.

You can configure the following services for TLS and Auth:

1. Controller
2. Segment Store
3. Zookeeper (optionally)
4. Bookkeeper (optionally)

For information about enabling TLS for Zookeeper and Bookeeper, refer to
their documentation here:
* [ZooKeeper SSL Guide](https://cwiki.apache.org/confluence/display/ZOOKEEPER/ZooKeeper+SSL+User+Guide)
* [BookKeeper - Encryption and Authentication using TLS](https://bookkeeper.apache.org/docs/latest/security/tls/)

Configuring security for Controllers and Segment Stores is discussed below.

**Controller**

Controller services can be configured in three different ways:

1. By specifying the configuration parameter values directly in the `controller.config.properties` file. For example,

   ```
   controller.security.tls.enable=true
   controller.security.tls.server.certificate.location=/etc/secrets/controller01.pem
   ```

2. By specifying configuration parameters as JVM system properties. This way of configuring Controller service is more 
relevant for container application deployment tools and orchestrators such as Docker Compose, Swarm and Kubernetes.

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

The following table lists the Controller's TLS and auth parameters and representative values, for quick reference. 
For a detailed description of these parameters, refer to [this](https://github.com/pravega/pravega/blob/master/documentation/src/docs/security/pravega-security-configurations.md) document.

 | Configuration Parameter| Example Value |
 |:-----------------------:|:-------------|
 | `controller.security.tls.enable` | true |
 | `controller.security.tls.server.certificate.location` | /etc/secrets/controller01-server-cert.crt |
 | `controller.security.tls.server.privateKey.location` | /etc/secrets/controller01-server-key.key |
 | `controller.security.tls.trustStore.location` | /etc/secrets/ca-cert.crt |
 | `controller.security.tls.server.keyStore.location` | /etc/secrets/controller01-server.keystore.jks |
 | `controller.security.tls.server.keyStore.pwd.location` | /etc/secrets/controller01-server.keystore.jks.password <sup>1</sup> |
 | `controller.zk.connect.security.enable` | false <sup>2</sup> |
 | `controller.zk.connect.security.tls.trustStore.location` | /etc/secrets/client.truststore.jks |
 | `controller.zk.connect.security.tls.trustStore.pwd.location` | /etc/secrets/client.truststore.jks.password |
 | `controller.security.auth.enable` | true |
 | `controller.security.pwdAuthHandler.accountsDb.location` <sup>3</sup> | /etc/secrets/password-auth-handler.database |
 | `controller.security.auth.delegationToken.signingKey.basis` | a-secret-value |

 [1]: This and other `.password` files are text files containing the password for the corresponding store.

 [2]: It is assumed here that Zookeeper TLS is disabled. You may enable it and specify the corresponding client-side 
 TLS configuration properties via the `controller.zk.*` properties.

 [3]: This configuration property is required when using the default Password Auth Handler only.

**Segment Store**

Segment store supports configuration via a properties file (`config.properties`) or JVM system properties. The table 
below lists its TLS and auth parameters and sample values. For a detailed discription of these parameters refer to
[this](https://github.com/pravega/pravega/blob/master/documentation/src/docs/security/pravega-security-configurations.md) document.

 | Configuration Parameter| Example Value |
 |:-----------------------:|:-------------|
 | `pravegaservice.security.tls.enable` | true |
 | `pravegaservice.security.tls.server.certificate.location` | /etc/secrets/segmentstore01-server-cert.crt |
 | `pravegaservice.security.tls.server.privateKey.location` | /etc/secrets/segmentstore01-server-key.key |
 | `pravegaservice.zk.connect.security.enable` | false <sup>2</sup> |
 | `pravegaservice.zk.connect.security.tls.trustStore.location` | /etc/secrets/client.truststore.jks |
 | `pravegaservice.zk.connect.security.tls.trustStore.pwd.location` | /etc/secrets/client.truststore.jks.password |
 | `autoScale.controller.connect.security.tls.enable` | true |
 | `autoScale.controller.connect.security.tls.truststore.location` | /etc/secrets/ca-cert.crt |
 | `autoScale.controller.connect.security.auth.enable` | true |
 | `autoScale.security.auth.token.signingKey.basis` | a-secret-value <sup>1</sup>|
 | `autoScale.controller.connect.security.tls.validateHostName.enable` | true |

[1]: It is assumed here that Zookeeper TLS is disabled. You may enable it and specify the corresponding client-side TLS configuration properties via the `pravegaservice.zk.*` properties.

[2]: The secret value you use here must match the same value used for other Controller and Segment Store services.

### Configuring TLS and Credentials on Client Side

After enabling and configuring TLS and auth on the server-side services, its time to update the clients,
so that the they can establish TLS connections with the servers and are allowed access.

For TLS, establish trust for the servers' certificates on the client side using one of the following ways:

  1. Supply the client library with the certificate of the trusted CA that has signed the servers' certificates.

     ```
     ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI("tls://<DNS-NAME-OR-IP>:9090")
                .trustStore("/etc/secrets/ca-cert")
                ...
                .build();
     ```

  2. Install the CA's certificate in the Java system key store.
  3. Create a custom truststore with the CA's certificate and supply it to the Pravega client application,
   via JVM system properties `javax.net.ssl.trustStore` and `javax.net.ssl.trustStorePassword`.

For auth, client-side configuration depends on the `AuthHandler` implementation used. If your server is configured to
use the default `PasswordAuthHandler`, you may supply the credentials as shown below.

  ```
  ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI("tls://<DNS-NAME-OR-IP>:9090")
                .trustStore("/etc/secrets/ca-cert")
                .credentials(new DefaultCredentials("changeit", "marketinganaylticsapp"))
                .build();
  ```

#### Server Hostname Verification

Hostname verification during TLS communications verifies that the DNS name to which the client connects matches the hostname specified in either of the following fields in the server's certificate:

* Common Name (`CN`) in the certificate's `Subject` field
* One of the `Subject Alternative Names` field entries

If the server certificates have a hostname assigned, you have used IP addresses as endpoints for the services, and those hostnames are not accessible from
the client nodes, you might need to add mappings of
IP addresses and DNS/Host names in the client-side operating system hosts file.

Alternatively, you may disable hostname verification by invoking `validateHostName(false)` of the ClientConfig builder. 
It is strongly recommended to avoid disabling hostname verification for production clusters.

### Having TLS and Auth Take Effect

To ensure TLS and auth parameters take effect, all the services on the server-side need to be restarted.
Existing client applications will need to be restarted as well, after they are reconfigured for TLS and auth.

For fresh deployments, starting the cluster and the clients after configuring TLS and auth, will automatically ensure they take effect.

## Conclusion

This document explained about how to enable security in a Pravega cluster running in distributed mode. 
Specifically, how to perform the following actions were discussed:

* Generating a CA (if needed)
* Generating server certificates and keys for Pravega services
* Signing the generated certificates using the generated CA
* Enabling and configuring TLS and auth on the server Side
* Setting up the `ClientConfig` on the client side for communicating with a Pravega cluster running with TLS and auth enabled
* Having TLS and auth take effect
