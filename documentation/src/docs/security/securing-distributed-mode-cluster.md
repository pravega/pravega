<!--
Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.

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

Regardless of the deployment option used, setting up Transport Layer Security (SSL/TLS) and client auth (short for
authentication and authorization) are important steps towards a secure Pravega deployment.

TLS encrypts client-server and internal communication. It also enables clients to authenticate the services running on the server nodes. Client auth enables the services to authenticate and authorize the clients. Pravega strongly recommends enabling both TLS and auth, for production clusters.

Setting up security - especially TLS - in a large cluster can be daunting at first. To make it easier, this document
provides step-by-step instructions on how to enable and configure security manually.

Depending on the deployment option used and your environment, you might need to modify the steps and commands to
suit your specific needs and policies.

## Setting Up SSL/TLS

There are broadly two ways of using TLS for client-server communications:

1. Setup Pravega to handle TLS directly. In this case, end-to-end traffic is encrypted.
2. Terminate TLS outside of Pravega, in an infrastructure component such as a reverse proxy or a load balancer. Traffic is encrypted until the terminating point and is in plaintext from there to Pravega.

Depending on the deployment option used, it might be easier to use one or the other approach. For example, if you
are deploying a Pravega cluster in Kubernetes, you might find approach 2 simpler and easier to manage. In a cluster deployed manually on hardware machines, it might be more convenient to use approach 1 in many cases.
The specifics of enabling TLS will also differ depending on the deployment option used.

Here, we describe the steps applicable for approach 1 in manual deployments. For approach 2, refer to the platform vendor's documentation.

At a high level, setting up TLS can be divided into three distinct stages:

1. Setting up a Certificate Authority (CA)
2. Obtaining server certificates and keys
3. Enabling TLS and Deploying certificates

They are described in detail in the following sub-sections.

**Before you Begin**

As the steps in this section use either OpenSSL or Java Keytool, install
OpenSSL and Java Development Kit (JDK) on the hosts that will be used to  generate
TLS certificates, keys, keystores are truststores.

**Note:**

* The examples shown in this section use command line arguments to pass all inputs to the command. To pass
sensitive command arguments via prompts instead, just exclude the corresponding option. For example,

  ```bash
  # Inputs are passed as command line arguments.
  $ keytool -keystore server01.keystore.jks -alias server01 -validity <validity> -genkey \
              -storepass <keystore-password> -keypass <key-password> \
              -dname <distinguished-name> -ext SAN=DNS:<hostname>,

  # Passwords and other arguments are entered interactively on the prompt in this case.
  $ keytool -keystore server01.keystore.jks -alias server01 -genkey
  ```
* A weak password `changeit` is used everywhere, for easier reading. Be sure to replace it with a strong and separate
password for each file.

### Stage 1: Setting Up a Certificate Authority (CA)

If you are going to use an existing public or internal CA service or certificate and key bundle, you may skip this part altogether, and go to [Obtaining Server Certificates and keys](#stage-2-obtaining-server-certificates-and-keys).

Here, we'll generate a CA in the form of a public/private key pair and a self-signed certificate.
Later, we'll use the CA certificate/key bundle to sign server certificates used in the cluster.

1. Generate a certificate and public/private key pair, for use as a CA.

   ```bash
   # All inputs provided using command line arguments
   $ openssl req -new -x509 -keyout ca-key -out ca-cert -days <validity> \
            -subj "<distinguished_name>" \
            -passout pass:<strong_password>

   # Sample command
   $ openssl req -new -x509 -keyout ca-key -out ca-cert -days 365 \
            -subj "/C=US/ST=Washington/L=Seattle/O=Pravega/OU=CA/CN=Pravega-CA" \
            -passout pass:changeit
   ```

2. Create a truststore containing the CA's certificate.

   This truststore will be used by external and internal clients. External clients are client applications connected
   to a Pravega cluster. Services running on server nodes play the role of internal clients when accessing other services.

   ```bash
   $ keytool -keystore client.truststore.jks -noprompt -alias CARoot -import -file ca-cert \
        -storepass changeit

   # Optionally, list the truststore's contents to verify everything is in order. The output should show
   # a single entry with alias name `caroot` and entry type `trustedCertEntry`.
   $ keytool -list -v -keystore client.truststore.jks -storepass changeit
   ```

At this point, the following CA and client truststore artifacts shall be  available:

| File | Description |
|:-----:|:--------|
| `ca-cert` | PEM-encoded X.509 certificate of the CA |
| `ca-key` | PEM-encoded file containing the CA's encrypted private key  |
| `client.truststore.jks` | A password-protected truststore file containing the CA's certificate |

### Stage 2: Obtaining Server Certificates and keys

This stage is about performing the following steps for each service.

1. Generating server certificates and keys
2. Generating a Certificate Signing Request (CSR)
3. Submitting the CSR to a CA and obtaining a signed certificate
4. Preparing a keystore containing the signed server certificate and the CA's certificate
5. Exporting the server certificate's private key

**Note:**

For services running on the same host, the same certificate can be used, if those services are accessed using the same hostname/IP address. Also, wildcard certificates can be used to share certificates across hosts. However, it is strongly recommended that separate certificates be used for each service.

The steps are:

1. Generate a public/private key-pair and a X.509 certificate for each service.

   This certificate is used for TLS connections with clients and by clients to  verifying the server's identity.

   ```bash
   $ keytool -keystore controller01.jks\
        -genkey -keyalg RSA -keysize 2048 -keypass changeit\
        -alias controller01 -validity 365\
        -dname "CN=controller01.pravega.io, OU=..., O=..., L=..., S=..., C=..."\
        -ext san=dns:controller01.pravega.io,ip:...\
        -storepass changeit

   # Optionally, verify the contents of the generated file
   $ keytool -list -v -keystore controller01.jks -storepass changeit
   ```

2. Generate a certificate signing request (CSR) for each service.

   It helps to think of a CSR as an application for getting a certificate signed by a trusted authority.

   A CSR is typically generated on the same server/node on which the service is planned to be installed. In some other environments, CSRs are generated in a central server and the resulting certificates are distributed to the services that need them.  

   ```bash
   $ keytool -keystore controller01.jks -alias controller01 -certreq -file controller01.csr \
             -storepass changeit

   # Optionally, inspect the contents of the CSR file. The CSR is created in Base-64 encoded `PEM` format.
   $ openssl req -in controller01.csr -noout -text
   ```

3. Submit the CSR to a CA and obtain a signed certificate for each service.

   If you are using a public or internal CA service, follow that CA's process for submitting the CSR and obtaining
   a signed certificate. To use the custom CA generated using the steps mentioned [earlier](#stage-1-setting-up-a-certificate-authority-ca) or an internal CA
   certificate/key bundle, use the following command, to generate a CA-signed server certificate in `PEM` format:

   ```bash
   $ openssl x509 -req -CA ca-cert -CAkey ca-key -in controller01.csr -out controller01.pem \
        -days 3650 -CAcreateserial -passin pass:changeit
   ```

4. Prepare a keystore containing the signed server certificate and the CA's certificate.

   ```bash
   # Import the CA certificate into a new keystore file.
   $ keytool -keystore controller01.server.jks -alias CARoot -noprompt \
           -import -file ca-cert -storepass changeit

   # Import the signed server certificate into the keystore.
   $ keytool -keystore controller01.server.jks -alias controller01 -noprompt \
           -import -file controller01.pem -storepass changeit
   ```

5. Export each server's key into a separate `PEM` file.

   This is a two step process.
   * First, convert the server's keystore in `.jks` format into `.p12` format.

     ```bash
     $ keytool -importkeystore
             -srckeystore controller01.jks \
             -destkeystore controller01.p12 \
             -srcstoretype jks -deststoretype pkcs12 \
             -srcstorepass changeit -deststorepass changeit
     ```
   * Then, export the private key of the server into a `PEM` file. Note that the generated `PEM` file is not protected
   by a password. The key itself is password-protected, as we are using the `-nodes` flag. So, be sure
   to protect it using operating system's technical controls as well as procedural controls.

     ```bash
     $ openssl pkcs12 -in controller01.p12 -out controller01.key.pem -passin pass:1111_aaaa -nodes
     ```

Step 5 concludes this stage, and the stage is now set for installing the certificates and other PKI material in Pravega.

The table below lists the key output of this stage. Note that you'll typically need one of each file per Pravega
service, but you may share the same file for services collocated on the same host for logistical or economical reasons.

| Files | Example File| Description |
|:-----:| :---:|:--------|
| Certificate in `PEM` format | `controller01.pem` file | The signed certificate to be used by the service.|
| Private key in `PEM` format | `controller01.key.pem` file  |  The private key to be used by the service. |
| Server keystore in `JKS` format | `controller01.server.jks` file | The keystore file to be used by the service. |


### Stage 3: Enabling TLS and Deploying Certificates

We'll discuss this in the [next](#enabling-tls-and-auth-in-pravega) section, together with other security configuration and setup.

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
   controller.auth.tlsEnabled=true
   controller.auth.tlsCertFile=/etc/secrets/controller01.pem
   ```
2. By specifying configuration values via corresponding environment variables. For example,

   ```bash
   # TLS_ENABLED environment variable corresponds to Controller configuration parameter
   # "controller.auth.tlsEnabled".
   $ export TLS_ENABLED: "true"
   ```

   To identify the environment variables corresponding to the configuration parameter, inspect the default
   `controller.config.properties` file and locate the variables.

   ```
   controller.auth.tlsEnabled=${TLS_ENABLED}
   controller.auth.tlsCertFile=${TLS_CERT_FILE}
   ```

3. By specifying configuration parameters as JVM system properties. This way of configuring Controller service is more relevant for container application deployment tools and orchestrators such as Docker Compose, Swarm and Kubernetes.

   ```
   # Example: docker-compose.yml file
   ...
   services:
      ...
      controller:
         environment:
             ...
             JAVA_OPTS:
                -dcontroller.auth.tlsEnabled=true
                -dcontroller.auth.tlsCertFile=...
                ...
      ...          
   ```

The following table lists the Controller's TLS and auth parameters and representative values, for quick reference. For a detailed description of these parameters, refer to [this](https://github.com/pravega/pravega/blob/master/documentation/src/docs/security/pravega-security-configurations.md) document.

 | Configuration Parameter| Example Value |
 |:-----------------------:|:-------------|
 | `controller.auth.tlsEnabled` | true |
 | `controller.auth.tlsCertFile` | /etc/secrets/controller01.pem |
 | `controller.auth.tlsKeyFile` | /etc/secrets/controller01.key.pem |
 | `controller.auth.tlsTrustStore` | /etc/secrets/ca-cert |
 | `controller.rest.tlsKeyStoreFile` | /etc/secrets/controller01.server.jks |
 | `controller.rest.tlsKeyStorePasswordFile` | /etc/secrets/controller01.server.jks.password <sup>1</sup> |
 | `controller.zk.secureConnection` | false <sup>2</sup> |
 | `controller.zk.tlsTrustStoreFile` | /etc/secrets/client.truststore.jks |
 | `controller.zk.tlsTrustStorePasswordFile` | /etc/secrets/client.truststore.jks.password |
 | `controller.auth.enabled` | true |
 | `controller.auth.userPasswordFile` <sup>3</sup> | /etc/secrets/password-auth-handler.inputfile |
 | `controller.auth.tokenSigningKey | a-secret-value |

 [1]: This and other `.password` files are text files containing the password for the corresponding store.

 [2]: It is assumed here that Zookeeper TLS is disabled. You may enable it and specify the corresponding client-side TLS configuration properties via the `controller.zk.*` properties.

 [3]: This configuration property is required when using the default Password Auth Handler only.

**Segment Store**

Segment store supports configuration via a properties file (`config.properties`) or JVM system properties. The table below lists its TLS and auth parameters and sample values. For a detailed discription of these parameters refer to [this](https://github.com/pravega/pravega/blob/master/documentation/src/docs/security/pravega-security-configurations.md) document.

 | Configuration Parameter| Example Value |
 |:-----------------------:|:-------------|
 | `pravegaservice.enableTls` | true |
 | `pravegaservice.certFile` | /etc/secrets/segmentstore01.pem |
 | `pravegaservice.keyFile` | /etc/secrets/segmentstore01.key.pem |
 | `pravegaservice.secureZK` | false <sup>2</sup> |
 | `pravegaservice.zkTrustStore` | /etc/secrets/client.truststore.jks |
 | `pravegaservice.zkTrustStorePasswordPath` | /etc/secrets/client.truststore.jks.password |
 | `autoScale.tlsEnabled` | true |
 | `autoScale.tlsCertFile` | /etc/secrets/segmentstore01.key.pem |
 | `autoScale.authEnabled` | true |
 | `autoScale.tokenSigningKey` | a-secret-value <sup>1</sup>|
 | `autoScale.validateHostName` | true |

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

Alternatively, you may disable hostname verification by invoking `validateHostName(false)` of the ClientConfig builder. It is strongly recommended to avoid disabling hostname verification for production clusters.

### Having TLS and Auth Take Effect

To ensure TLS and auth parameters take effect, all the services on the server-side need to be restarted.
Existing client applications will need to be restarted as well, after they are reconfigured for TLS and auth.

For fresh deployments, starting the cluster and the clients after configuring TLS and auth, will automatically ensure they take effect.

## Conclusion

This document explained about how to enable security in a Pravega cluster running in distributed mode. Specifically, how to perform the following actions were discussed:

* Generating a CA (if needed)
* Generating server certificates and keys for Pravega services
* Signing the generated certificates using the generated CA
* Enabling and configuring TLS and auth on the server Side
* Setting up the `ClientConfig` on the client side for communicating with a Pravega cluster running with TLS and auth enabled
* Having TLS and auth take effect
