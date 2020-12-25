<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Creating TLS Certificates and Keys

This document describes how to create TLS certificates and keys for the server components. 

## Overview

A TLS server certificate is an X.509 digital certificate that binds the server's identity (such as the server's 
DNS name or IP address or both) to the server's public key. As such, the public key in the server's certificate 
identifies the server. The public key is part of the public-private key pair used for asymmetric encryption during the 
TLS handshake. 

While the server's certificate is public and is available to clients, the server's private key must be protected and 
should be available only to the server. Ideally, each server should use its own public-private key pair, as using the same 
private key (together with the public key/TLS certificate) across multiple servers increases the risk that the 
private key will be compromised. 

A Pravega cluster's certificate and key requirements may vary depending on deployment options used for the cluster: 
1. Use a separate TLS certificate and key pair for each host/machine. Manual deployments supports this configuration, 
   as well as the rest of the  configurations below. In this case, each certificate identifies the respective server. 
   This is the most secure option. 
2. Use two pairs of certificate and key, one pair for the Controllers and the other for the Segment Stores. 
   Each certificate contains the DNS names and/or IP addresses of respective servers. Pravega Kubernetes deployment supports 
   this configuration, as well as the next one. 
3. Use the same certificate and key pair for all Controllers and Segment Store Create a single set of certificate and key and use the same set across all the Controllers and Segment Stores. The 
   certificate will need to contain the identity of all the respective servers. In this case, a single ceritificate/
   private key is sufficient. 
   
Depending on the chosen configuration, multiple pairs of public key server certificate and private key may be required, 
and creation and signing process described below will need to be repeated for each pair. 

Also, rather than terminating TLS at the Pravega services, one may choose to offload TLS termination to an external 
infrastructural component like reverse proxy, Kubernetes Ingress or load balancer. If that's the case, the subject of the 
TLS certificates and keys shall be that external component and not a Pravega server, and additional steps may be required 
to ensure that other parts of the infrastructure can communicate with it. Refer to the Platform vendor's documentation, 
such as [this](https://cloud.google.com/kubernetes-engine/docs/concepts/ingress) one from Google Kubernetes Engine, 
for instructions.

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

## Setting up a Certificate Authority (CA)

If you are going to use an existing public or internal CA service or certificate and key bundle, you may skip 
this part altogether, and go to [Obtaining Server Certificates and keys](#stage-2-obtaining-server-certificates-and-keys).

Here, we'll create a CA in the form of a public/private key pair and a self-signed certificate.
Later, we'll use this CA certificate/key bundle to sign the servers' certificates.

1. Generate a CA certificate and public/private key pair.

   ```bash
   $ openssl req -new -x509 -keyout <private-key-file-name> -out <ca-cert-file-name> -days <validity> \
            -subj "<distinguished_name>" \
            -passout pass:<private-key-password>

   # Sample command
   $ openssl req -new -x509 -keyout ca-key.key -out ca-cert.crt -days 365 \
            -subj "/C=US/ST=Washington/L=Seattle/O=Pravega/OU=CA/CN=Pravega-Stack-CA" \
            -passout pass:changeit
   ```

2. Create a truststore containing the CA's certificate.

   This truststore will be used by external and internal clients. External clients are client applications connected
   to a Pravega cluster. Services running on server nodes play the role of internal clients when accessing other services.

   ```bash
   $ keytool -keystore <java-truststore-file-name> -noprompt -alias <trusted-cert-entry-alias> -import -file <ca-cert-file-name> \
           -storepass <java-truststore-file-password>
   
   $ Sample command
   $ keytool -keystore client.truststore.jks -noprompt -alias caroot -import -file ca-cert.crt \
        -storepass changeit

   # Optionally, list the truststore's contents to verify everything is in order. The output should show
   # a single entry with alias name `caroot` and entry type `trustedCertEntry`.
   $ keytool -list -v -keystore client.truststore.jks -storepass changeit
   ```

At this point, the following CA and client truststore artifacts shall be  available:

| File | Description | Command for Inspecting the file's Contents |
|:-----:|:--------|:--------------|
| `ca-cert.crt` | PEM-encoded X.509 certificate of the CA | `$ openssl x509 -in ca-cert.crt -text -noout` |
| `ca-key.key` | PEM-encoded file containing the CA's encrypted private key  | `$ openssl pkcs8 -inform PEM -in ca-key.key -topk8` |
| `client.truststore.jks` | A password-protected truststore file containing the CA's certificate | `$ keytool -list -v -keystore client.truststore.jks -storepass 1111_aaaa` |

## Creating and Signing Server Certificates and Private Keys

This stage is about performing the following steps for each server certificate/key pair.

1. Generating server certificates and keys
2. Generating a Certificate Signing Request (CSR)
3. Submitting the CSR to a CA and obtaining a signed certificate
4. Preparing a keystore containing the signed server certificate and the CA's certificate
5. Exporting the server certificate's private key

**Note:** For services running on the same host, the same certificate can be used, if those services are 
accessed using the same hostname/IP address. Also, wildcard certificates can be used to share certificates across 
hosts. However, it is strongly recommended that separate certificates be used for each service.

The steps are:

1. Generate a public/private key-pair and a X.509 certificate for each service.

   This certificate is used for TLS connections with clients and by clients to  verifying the server's identity.

   ```bash
   $ keytool -storetype JKS -keystore <java-keystore-file-name> -storepass <java-keystore-password>\
           -genkey -keyalg <key-algorithm> -keysize <key-size> -keypass <private-key-password>\
           -alias <alias> -validity <validity>\
           -dname "<distinguished-name>"\
           -ext SAN=dns:<dns-name1>,dns:<dns-name1>,ip:<ip-address1>
   
      $ keytool -storetype JKS -keystore server.keystore.jks -storepass changeit\
              -genkey -keyalg RSA -keysize 2048 -keypass changeit\
              -alias server -validity 365\
              -dname "CN=server.pravega.io, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown"\
              -ext SAN=dns:server.abc.com,ip:127.0.0.1
   
   # Optionally, verify the contents of the generated file
   $ keytool -list -v -storetype JKS -keystore server.keystore.jks -storepass changeit
   ```

2. Generate a certificate signing request (CSR) for each service.

   It helps to think of a CSR as an application for getting a certificate signed by a trusted authority.

   A CSR is typically generated on the same server/node on which the service is planned to be installed. In some other environments, CSRs are generated in a central server and the resulting certificates are distributed to the services that need them.  

   ```bash
   $ keytool -keystore controller01.jks -alias controller01 -certreq -file controller01.csr \
                -storepass changeit
   
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