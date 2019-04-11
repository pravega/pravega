<!--
Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Setting Up Security for a Distributed Mode Cluster

In the [distributed mode](../deployment/deployment.md#pravega-modes) of running a Pravega cluster, each service runs
separately on one or more processes, usually spread across multiple machines. The deployment options of this mode
include:

1. A manual deployment in hardware or virtual machines
2. Containerized deployments of these types:
    *   A Kubernetes native application deployed using the Pravega Operator
    * A Docker Compose application deployment
    * A Docker Swarm based distributed deployment

Regardless of the deployment option used, setting up Transport Layer Security (SSL/TLS) and client auth (short for
authentication and authorization) are important steps towards a secure Pravega deployment.

TLS encrypts client-server and internal communication. It also enables clients to authenticate the services running on the server nodes. Client auth enables the services to authenticate and authorize the clients. Pravega strongly recommends enabling both TLS and auth for production clusters.

Setting up security - especially TLS - in a large cluster can be daunting at first. To make it easier, this document
provides step-by-step instructions on how to enable and configure security manually.

Depending on the deployment option used and your environment, you might need to modify the steps and commands to
suite your specific needs and policies. Moreover, some of the deployment options open up additional ways of securing
a cluster, as we will briefly touch on in the next sub-section.

## Setting up SSL/TLS

There are broadly two ways of using TLS for client-server communications:

1. Setup Pravega to handle TLS directly. In this case, end-to-end tracleaffic is encrypted.
2. Terminate TLS outside of Pravega, in an infrastructure component such as a reverse proxy or a load balancer. Traffic is encrypted until the terminating point and is in plaintext from there to Pravega.

Depending on the deployment option used, it might be easier to use one or the other approach. For example, if you
are deploying a Pravega cluster in Kubernetes, you might find approach 2 simpler and easier to manage. In a cluster deployed manually on hardware machines, it might be more convenient to use approach 1 in many cases.
The specifics of enabling TLS will also differ depending on the deployment option used.

Here, we describe the steps applicable for approach 1 in manual deployments. For approach 2, refer to the platform vendor's documentation.

At a high level, setting up TLS can be divided into three distinct stages:

1. Setting up a Certificate Authority (CA)
2. Obtaining server certificates and keys
3. Deploying certificates and enabling TLS in Pravega

They are described in detail in the following sub-sections.

**Before you Begin:**

You need to have OpenSSL and Java Development Kit (JDK) installed, as the commands in this section are based on OpenSSL and Java Keytool.

NOTE:

* The examples shown in this section use command line arguments to pass all inputs to the command. To pass
sensitive command arguments via prompts instead, just exclude the corresponding option. For example,

  ```
  # Inputs passed as command line arguments
  $ keytool -keystore server01.keystore.jks -alias server01 -validity <validity> -genkey \
              -storepass <keystore-password> -keypass <key-password> \
              -dname <distinguished-name> -ext SAN=DNS:<hostname>,

  # Passwords and other arguments entered interactively on the prompt
  $ keytool -keystore server01.keystore.jks -alias server01 -genkey
  ```
* A weak password `changeit` is used everywhere, for easier reading. Be sure to replace it with a strong and separate
password for each file.

### Stage 1: Setting up a Certificate Authority (CA)

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

   ```
   $ keytool -keystore client.truststore.jks -noprompt -alias CARoot -import -file ca-cert \
        -storepass changeit

   # Optionally, list the truststore's contents to verify everything is in order. The output should show
   # a single entry with alias name `caroot` and entry type `trustedCertEntry`.
   $ keytool -list -v -keystore client.truststore.jks -storepass changeit
   ```

That's it! You have the following CA and client truststore artifacts:

| File | Description |
|:-----:|:--------|
| ca-cert | PEM-encoded X.509 certificate of the CA |
| ca-key | PEM-encoded file containing the CA's encrypted private key  |
| client.truststore.jks | A password-protected truststore file containing the CA's certificate |

### Stage 2: Obtaining Server Certificates and keys

This stage is about performing the following steps for each service.

1. Generating server certificates and keys
2. Generating a Certificate Signing Request (CSR)
3. Submitting the CSR to a CA and obtaining a signed certificate
4. Preparing a keystore containing the signed server certificate and the CA's certificate
5. Exporting the server certificate's private key

NOTE:

For services running on the same host, you may use the same certificate, if those services are accessed using
the same hostname/IP address. You may even use wildcard certificates to share certificates across nodes. However,
it is strongly recommended that you use separate certificates for each service.

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

   # Optionally, verify the contents of the generated file:
   $ keytool -list -v -keystore controller01.jks -storepass changeit
   ```

2. Generate a certificate signing request (CSR) for each service.

   It helps to think of a CSR as an application for getting a certificate signed by a trusted authority.

   A CSR is typically generated on the same server/node on which the service is planned to be installed. In some other environments, CSRs are generated in a central server and the resulting certificates are distributed to the services that need them.  

   ```
   $ keytool -keystore controller01.jks -alias controller01 -certreq -file controller01.csr \
       -storepass changeit

   # Optionally, inspect the contents of the CSR file. The CSR is created in Base-64 encoded PEM format.
   $ openssl req -in controller01.csr -noout -text
   ```

3. Submit the CSR to a CA and obtain a signed certificate for each service.

   If you are using a public or internal CA service, follow that CA's process for submitting the CSR and obtaining
   a signed certificate. To use the custom CA generated using the steps mentioned earlier or an internal CA
   certificate/key bundle, use the following command, to generate a CA-signed server certificate in PEM format:

   ```
   $ openssl x509 -req -CA ca-cert -CAkey ca-key -in controller01.csr -out controller01.pem \
        -days 3650 -CAcreateserial -passin pass:changeit
   ```

4. Prepare a keystore containing the signed server certificate and the CA's certificate.

   ```
   # Import the CA certificate into a new keystore file.
   $ keytool -keystore controller01.server.jks -alias CARoot -noprompt \
           -import -file ca-cert -storepass changeit

   # Import the signed server certificate into the keystore.
   $ keytool -keystore controller01.server.jks -alias controller01 -noprompt \
           -import -file controller01.pem -storepass changeit
   ```

5. Export each server's key into a separate .pem file.

   This is a two step process. First, convert the server's keystore in `.jks` format into `.p12` format.

   ```
   keytool -importkeystore -srckeystore controller01.jks  \
                           -destkeystore controller01.p12 \
                           -srcstoretype jks -deststoretype pkcs12 \
                           -srcstorepass changeit -deststorepass changeit
   ```

   Then, export the private key of the server into a `.pem` file. Note that the generated `.pem` file is not protected
   by a password, even though the key itself is password-protected, as we are using the `-nodes` flag. So, be sure
   to protect it using operating system's technical controls as well as procedural controls.

   ```
   openssl pkcs12 -in controller01.p12 -out controller01.key.pem -passin pass:1111_aaaa -nodes
   ```

The last step concludes this stage, and you should now be all set to install the certificates and other material in Pravega.

The table below lists the key output of this stage. Note that you'll typically need one of each file per Pravega
service, but you may share the same file for services collocated on the same host for logistical or economical reasons.

| Files | Example File| Description |
|:-----:| :---:|:--------|
| Certificate in PEM format | `controller01.pem` file | The signed certificate to be used by the service.|
| Private key in PEM format | `controller01.key.pem` file  |  The private key to be used by the service. |
| Server keystore in JKS format | `controller01.server.jks` file | The keystore file to be used by the service. |


### Deploying certificates and enabling TLS in Pravega

We'll discuss this in the next section, together with other security configuration and setup.

## Enabling TLS and Auth in Pravega

Enabling TLS and Auth in Pravega involves the following steps:

1. Configuring TLS and Auth Parameters for the Services
2. Configuring TLS and Credentials on the Client Side
3. Having the TLS and Auth parameters take effect

Each of the above tasks are discussed in the following sub-sections.

### Configuring TLS and Auth Parameters for for the Services

This step is about using the certificates, keys, keystores and truststores generated earlier to configure
TLS and Auth for the services on the server side.

Note that if you enable TLS and Auth on one service, you must enable them for all the other services too. Pravega
does not support using TLS and Auth for only some instances of the services.

You can configure the following services for TLS and Auth:

1. Controller
2. Segment Store
3. Zookeeper (optionally)
4. Bookkeeper (optionally)

Let's discuss about #1 and #2 below. For information about enabling TLS for Zookeeper and Bookeeper, refer to
their documentation here:
* [ZooKeeper SSL Guide](https://cwiki.apache.org/confluence/display/ZOOKEEPER/ZooKeeper+SSL+User+Guide)
* [BookKeeper - Encryption and Authentication using TLS](https://bookkeeper.apache.org/docs/latest/security/tls/)

**Controller Service:**

Controller services can be configured in three different ways:

1. By specifying the configuration parameter values directly in the `controller.config.properties` file. For example,

   ```
   controller.auth.tlsEnabled=true
   controller.auth.tlsCertFile=/etc/secrets/controller01.pem
   ```
2. By specifying configuration values via corresponding environment variables. For example,

   ```
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

3. By specifying configuration parameters (such as `controller.auth.tlsEnabled`) as JVM system properties. This is not
applicable for manual deployment and is relevant for container application deployment tools and orchestrators such as Docker Compose, Swarm and Kubernetes.


The table below lists the Controller services' TLS and auth parameters and representative values for quick reference. For a detailed discription of these parameters refer to [this](https://github.com/pravega/pravega/blob/master/documentation/src/docs/security/pravega-security-configurations.md) document.

 | Configuration Parameter| Sample Value |
 |:-----------------------:|:-------------|
 | controller.auth.tlsEnabled | `true` |
 | controller.auth.tlsCertFile | `/etc/secrets/controller01.pem` |
 | controller.auth.tlsKeyFile | `/etc/secrets/controller01.key.pem` |
 | controller.auth.tlsTrustStore | `/etc/secrets/ca-cert` |
 | controller.rest.tlsKeyStoreFile | `/etc/secrets/controller01.server.jks` |
 | controller.rest.tlsKeyStorePasswordFile | `/etc/secrets/controller01.server.jks.password`<sup>1</sup> |
 | controller.zk.secureConnection | `false`<sup>2</sup> |
 | controller.zk.tlsTrustStoreFile | `/etc/secrets/client.truststore.jks` |
 | controller.zk.tlsTrustStorePasswordFile | `/etc/secrets/client.truststore.jks.password` |
 | controller.auth.enabled | `true` |
 | controller.auth.userPasswordFile<sup>3</sup> | `/etc/secrets/password-auth-handler.input` |
 | controller.auth.tokenSigningKey | `a-secret-value` |

 <sup>1</sup>: This and other `.password` files are text files containing the password for the corresponding store.

 <sup>2</sup>: The assumption is that Zookeeper TLS is disabled. You may enable it and specify the corresponding client-side TLS configuration properties via the `controller.zk.*` properties.

 <sup>3</sup>: This configuration property is required when using the default Password Auth Handler only.

**Segment Store Service:**

Segment store supports configuration via a properties file (`config.properties`) or JVM system properties. The table below lists its TLS and auth parameters and sample values. For a detailed discription of these parameters refer to [this](https://github.com/pravega/pravega/blob/master/documentation/src/docs/security/pravega-security-configurations.md) document.

 | Configuration Parameter| Sample Value |
 |:-----------------------:|:-------------|
 | pravegaservice.enableTls | `true` |
 | pravegaservice.certFile | `/etc/secrets/segmentstore01.pem` |
 | pravegaservice.keyFile | `/etc/secrets/segmentstore01.key.pem` |
 | pravegaservice.keyFile | `/etc/secrets/segmentstore01.key.pem` |
 | pravegaservice.secureZK | `false`<sup>2</sup> |
 | pravegaservice.zkTrustStore | `/etc/secrets/client.truststore.jks` |
 | pravegaservice.zkTrustStorePasswordPath | `/etc/secrets/client.truststore.jks.password` |
 | autoScale.tlsEnabled | `true` |
 | autoScale.tlsCertFile | `/etc/secrets/segmentstore01.key.pem` |
 | autoScale.authEnabled | `true` |
 | autoScale.tokenSigningKey | `a-secret-value` <sup>1</sup>|
 | autoScale.validateHostName | `true` |

<sup>1</sup>: The assumption is that Zookeeper TLS is disabled. You may enable it and specify the corresponding client-side TLS configuration properties via the `pravegaservice.zk.*` properties.

<sup>2</sup>: The secret value you use here must match the same value used for other Controller and Segment Store services.

### Configuring TLS and Credentials on the Client Side

After you have enabled TLS and Auth and configured them for server-side services, you need to update any existing client
applications to use TLS and supply their credentials. This sub-section described how to do that.

For TLS, you'll need to establish trust for the servers' certificates on the client sde. There are multiple ways of
doing that:

  1. Supplying the client library with the certificate of the trusted CA that has signed the servers' certificates.

  ```
  ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI("tls://<controller-hostname-or-ip>:9090")
                .trustStore(Constants.TRUSTSTORE_PATH)
                ...
                .build();
  ```

  2. Installing the CA's certificate in the Java system key store.
  3. Creating a custom truststore with the CA's certificate and supplying it to the Pravega client application via the
system properties `javax.net.ssl.trustStore` and `javax.net.ssl.trustStorePassword`.

For Auth, client-side configuration depends on the `AuthHandler` implementation used. If your server is configured to
use the default `PasswordAuthHandler`, you may supply the credentials as shown below.

  ```
  ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI("tls://<controller-hostname-or-ip>:9090")
                .trustStore(Constants.TRUSTSTORE_PATH)
                .credentials(new DefaultCredentials("changeit", "marketinganaylticsapp"))
                .build();

  ```

#### Hostname Verification

Hostname verification during TLS communications verifies that the DNS name to which the client connects matches the hostname specified in either of the following fields in the server's certificate:

* Common Name (`CN`) in the certificate's `Subject` field
* One of the `Subject Alternative Names` field entries

If the server certificates have a hostname assigned, you have used IP addresses as endpoints for the services, and those hostnames are not accessible from 
the client nodes, you might need to add mappings of
IP addresses and DNS/Host names in the client-side operating system hosts file.

Alternatively, you may disable hostname verification by invoking `validateHostName(false)` of the ClientConfig builder. However, we strongly recommended avoiding doing that, especially for production environments.

### Having the TLS and Auth parameters take effect

If you are enabling TLS and Auth in an existing Pravega cluster, having the security parameters take effect simply requires
restarting the servicesm after making the configuration changes. Any running client applications will need to be restarted as well, after making client-side changes as described earlier.

For fresh deployments, just starting the cluster after configuring TLS and Auth, should be enough.

## Conclusion

In this document, we saw how to enable security in a Pravega cluster running in distributed mode. Specifically, we discussed how to:
* Generate a CA (if needed),
* Generate server certificates and keys for Pravega services,
* Sign the generated certificates using the generated CA,
* Enable and configure TLS and auth on the server Side,
* Setup the `ClientConfig` on the client side for communicating with a Pravega, cluster running with TLS and auth enabled, and
* Have TLS and auth take effect.
