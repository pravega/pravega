# Setting Up Security for a Distributed Mode Cluster

In the Distributed Mode of running a cluster, each component runs separately on one or more processes, usually distributed across multiple machines. The deployment options in this mode include:

* A manual deployment in hardware or virtual machines
* Containerized deployments of these types:
  * A Kubernetes native application deployed using the Pravega Operator
  * A Docker Compose application deployment
  * A Docker Swarm based distributed deployment

Regardless of the deployment option used, setting up Transport Layer Security (SSL/TLS) for as well as client authentication and authorization (`Auth` in short) are important steps towards a secure deployment. TLS enables encryption of client-server and inter-component traffic and the clients to authenticate the server. Client `auth` enables the servers to authenticate and authorize the clients. Pravega strongly recommends always enabling security for production clusters.

Setting up security - especially TLS - in a large cluster can be daunting at first. To make it easier, this document provides step-by-step instructions on how to enable security when deloying a Pravega cluster manually.

Depending on the deployment option used and your environment, you might need to modify the steps and commands  to suite your specific needs and policies. Also, some of the deployment options open up additional ways of securing a cluster, as we will see in the next sub-section; We'll not discuss those ways in detail in this document, for the sake of brevity.

## Setting up SSL/TLS

There are two common ways of using TLS for client-server communications:

1. Setup Pravega to handle TLS directly.
2. Terminate TLS outside of Pravega, in an infrastructure component such as a reverse proxy or a Load Balancer.

Depending on the deployment option used, it might be easier to use one or the other approach. For instance, if you are deploying a Pravega cluster in Kubernetes, you might find using approach 2 simpler to use and manage. Also, the specifics of how to enable TLS will differ depending on the deployment option used. This document focuses on approach 1. For approach 2, refer to the vendor's documentation.

At a high level, setting up TLS in Pravega can be divided into three distinct stages:

1. Setting up a Certificate Authority (CA)
2. Obtaining server certificates and keys
3. Deploying certificates and enabling TLS in Pravega

The following sub-sections discuss the steps required for each of these phases. For setting up Zookeeper security, refer to the security documentation of Zookeeper.

**Before you Begin:**

You need to have the following installed on the server(s), as the commands in this section use either OpenSSL or Java Keytool.
* OpenSSL
* A supported version of Java Development Kit (JDK)

Also, note the following:

* In the examples shown in this section, we use command line arguments to pass all inputs to the command. To pass sensitive command arguments via prompts instead, just exclude the corresponding option. For example,

  ```
  # Inputs passed as command line arguments.
  $ keytool -keystore server01.keystore.jks -alias server01 -validity <validity> -genkey \
              -storepass <keystore-password> -keypass <key-password> \
              -dname <distinguished-name> -ext SAN=DNS:<hostname>,

  # Passwords and other arguments entered interactively on the prompt.
  $ keytool -keystore server01.keystore.jks -alias server01 -genkey
  ```
* In the samples below, a weak password `changeit` is used everywhere for easier reading. Be sure to replace it with strong and separate passwords for each file.

### Stage 1: Setting up a Certificate Authority (CA)

If you are going to use an existing public or internal CA service or certificate and key bundle, you may skip this stage.

In this stage, we'll generate a CA in the form of a public-private key pair and a self-signed signed certificate. Later, we'll use the CA to sign other certificates used in the cluster.

1. Create a certificate and key for use as a CA.

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

   This truststore will be used by external and internal clients. External clients are client applications connected to a Pravega cluster. Internal clients are services on the server playing the role of a client in internal communications.

   ```
   $ keytool -keystore client.truststore.jks -noprompt -alias CARoot -import -file ca-cert \
        -storepass changeit

   # Optionally, list the truststore's contents to verify everything is in order. The output should show
   # a single entry with alias name `caroot` and entry type `trustedCertEntry`.
   $ keytool -list -v -keystore client.truststore.jks -storepass changeit
   ```

That's it! You now have the following CA and client truststore artifacts:

| File | Description |
|:-----:|:--------|
| ca-cert | A file containing CA's certificate in .pem format|
| ca-key |  A password-protected file containing CA's private key  |
| client.truststore.jks | A password-protected truststore file containing the CA's certificate |

### Stage 2: Obtaining Server Certificates and keys

This phase is doing the following for each service:

1. Generating server certificates and keys
2. Generating a Certificate Signing Request (CSR)
3. Submitting the CSR to a CA and obtain a signed certificate
4. Preparing a keystore containing the signed server certificate and the CA's certificate
5. Exporting the server certificate's private key

Note:

* For services running on the same host, you may use the same certificate if those services are exposed using the same hostname/IP address. You may even use wildcard certificates to share certificates across hosts. However, it is strongly recommended that you use seperate certificates for each service.

Let's see the steps in detail below.

1. Generate keys and certificates for each service. This certificate is used for establishing TLS as well as for verifying the server's identity.

   ```bash
   $ keytool -keystore controller01.jks\
    -genkey -keyalg RSA -keysize 2048 -keypass changeit\
    -alias controller01 -validity 365\
    -dname "CN=controller01.pravega.io, OU=Pravega, O=Company, L=Locality, S=StateOrProvince, C=Country"\
    -ext san=dns:controller01.pravega.io,ip:<IP_ADDRESS>\
    -storepass changeit

   # Optionally, verify the contents of the generated file:
   $ keytool -list -v -keystore controller01.jks -storepass changeit
   ```

2. Generate CSR for each service. It helps to think of a CSR as an application for getting a certificate signed by a trusted authority.

   A CSR is typically generated on the same server on which the service is planned to be installed. Some organizations generate these outside of the servers in a central location and then distribute the resulting certificates to the services for use.

   ```
   $ keytool -keystore controller01.jks -alias controller01 -certreq -file controller01.csr \
       -storepass changeit

   # Optionally, inspect the contents of the CSR file. The CSR is created in Base-64 encoded PEM format.
   $ openssl req -in controller01.csr -noout -text
   ```

3. Submit the CSR to a CA and obtain a signed certificate for each service.

   If you are using a public or internal CA service, follow that CA's process for submitting the CSR and obtaining a signed certificate. To use the custom CA generated using the steps mentioned earlier or an internal CA certificate/key bundle, use the following command to generate a CA-signed server certificate in PEM format:

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

   Then, export the private key of the server into a `.pem` file. Note that the generated .pem file is not protected by a password, even though the key itself is password-protected, as we are using the `-nodes` flag. So, be sure to protect it using operating system's technical controls as well as procedural controls.

   ```
   openssl pkcs12 -in controller01.p12 -out controller01.key.pem -passin pass:1111_aaaa -nodes
   ```

Step 5 concludes this stage, and you are now all set to deploy the certificates and other material in Pravega.

The table below lists the key output of this stage. Note that you'll typically need one of each file per Pravega service, but you may share the same file for services collocated on the same host for logistical or cost reasons.

| Files | Example File| Description |
|:-----:| :---:|:--------|
| Certificate in PEM format | `controller01.pem` file | The signed certificate to be used by the service.|
| Private key in PEM format | `controller01.key.pem` file  |  The private key to be used by the service. |
| Server keystore in JKS format | `controller01.server.jks` file | The keystore file to be used by the service. |

### Deploying certificates and enabling TLS in Pravega

We'll discuss this in the section titled Enabling Security in Pravega, together with other security configuration and setup.

## Enabling Security in Pravega

Here, we discuss about these tasks:

1. Configuring TLS and Auth Parameters on the Server
2. Configuring TLS and credentials on the client
3. Having the TLS and Auth parameters take effect


### Configuring TLS and Auth Parameters on the Server

This task is about using the certificates, keys, keystores and truststores to configure TLS on the server side, and configuring Auth.

We'll need to configure the following services on the server side:

1. Controller services
2. Segment Store services
3. Zookeeper services (Optionally)

Lets' discuss about #1 and #2 below. For information about turning TLS on for Zookeeper, refer to its [SSL](https://cwiki.apache.org/confluence/display/ZOOKEEPER/ZooKeeper+SSL+User+Guide) documentation.

**Controller Service:**

Controller services can be configured in three distinct ways:

1. By specifying the configuration parameter values directly in the `controller.config.properties` file.

   ```
   controller.auth.tlsEnabled=true
   controller.auth.tlsCertFile=/etc/secrets/controller01.pem
   ```
2. By specifying configuration parameters via corresponding environment variables. For example,

   ```
   # TLS_ENABLED environment variable corresponds to Controller configuration parameter
   # "controller.auth.tlsEnabled".
   $ export TLS_ENABLED: "true"
   ```

   To identify the environment variables corresponding to the configuration parameter, inspect the default `controller.config.properties` file and locate the variables.

   ```
   controller.auth.tlsEnabled=${TLS_ENABLED}
   controller.auth.tlsCertFile=${TLS_CERT_FILE}
   ```

3. By specifying configuration parameters (such as `controller.auth.tlsEnabled`) as JVM system properties. This is not applicable for manual deployment and is relevant for container application deployment tools and orchestrators such as Docker Compose, Swarm and Kubernetes.


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

## Setting up Client Authentication and Authorization

On the client application side, you'll need to establish trust for the servers' certificates. There are multiple ways of doing that:

* Supplying the client library with the certificate of the trusted CA that has signed the servers' certificates.

  ```
  ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI("tls://<controller-hostname-or-ip>:9090")
                .trustStore(Constants.TRUSTSTORE_PATH)
                ...
                .build();
  ```

* Installing the CA's certificate in the Java system key store.
* Creating a custom truststore with the CA's certificate and supplying it to the Pravega client application via the system properties `javax.net.ssl.trustStore` and `javax.net.ssl.trustStorePassword`.

Further, when using the default Auth Handler implementation - the Pasword AuthHandler, supply the account credentials using Pravega ClientConfig, as shown below:

  ```
  ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI("tls://<controller-hostname-or-ip>:9090")
                .trustStore(Constants.TRUSTSTORE_PATH)
                .credentials(new DefaultCredentials("changeit", "marketinganaylticsapp"))
                .build();

  ```
