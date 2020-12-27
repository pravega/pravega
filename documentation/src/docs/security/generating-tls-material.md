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
DNS name or IP address or both) to the server's public key. The public key is part of the public-private key pair used 
for asymmetric encryption during the TLS handshake. 

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
* A weak password `changeit` is used everywhere in this document for easier reading. Be sure to replace it with 
a strong and separate password for each file.

## Setting up a Certificate Authority (CA)

If you are going to use an existing public or internal CA service or certificate and key bundle, you may skip 
this part altogether, and go to [Obtaining Server Certificates and keys](#stage-2-obtaining-server-certificates-and-keys).

Here, we'll create a CA in the form of a public/private key pair and a self-signed certificate.
Later, we'll use this CA certificate/key bundle to sign the servers' certificates.

1. Generate a CA certificate and public/private key pair. 

   ```bash
   $ openssl req -new -x509 -keyout <ca-private-key-file-path> -out <ca-cert-file-path> -days <validity-in-days> \
            -subj "<distinguished_name>" \
            -passout pass:<ca-private-key-password>

   # Example command
   $ openssl req -new -x509 -keyout ca-key.key -out ca-cert.crt -days 365 \
            -subj "/C=US/ST=Washington/L=Seattle/O=Pravega/OU=CA/CN=Pravega-Stack-CA" \
            -passout pass:changeit
   ```
  
   The command above will generate two PEM-encoded files:
   * A file containing the encrypted private key 
   * A file containing the CA certificate
   
2. Create a Java truststore containing the CA's certificate.

   The truststore we generate here will be used by external and internal TLS clients for determining whether to 
   trust the server's certificate. External clients are client applications connected to a Pravega cluster. Services 
   running on server nodes play the role of internal clients when accessing other services.

   ```bash
   $ keytool -keystore <java-truststore-file-name> -noprompt -alias <trusted-cert-entry-alias> -import -file <ca-cert-file-name> \
           -storepass <java-truststore-file-password>
   
   $ Example command
   $ keytool -keystore client.truststore.jks -noprompt -alias caroot -import -file ca-cert.crt \
        -storepass changeit

   # Optionally, list the truststore's contents to verify everything is in order. The output should show
   # a single entry with alias name `caroot` and entry type `trustedCertEntry`.
   $ keytool -list -v -keystore client.truststore.jks -storepass changeit
   ```

At this point, the following CA and truststore artifacts shall be  available:

| File | Description | Example command for inspecting the file's Contents |
|:-----:|:--------|:--------------|
| `ca-cert.crt` | PEM-encoded X.509 certificate of the CA | `$ openssl x509 -in ca-cert.crt -text -noout` |
| `ca-key.key` | PEM-encoded file containing the CA's encrypted private key  | `$ openssl pkcs8 -inform PEM -in ca-key.key -topk8` |
| `client.truststore.jks` | A password-protected truststore file containing the CA's certificate | `$ keytool -list -v -keystore client.truststore.jks -storepass changeit` |

## Creating and Signing Server Certificates and Private Keys

This stage is about preparing a set of server certificate and private key, and getting it signed by a CA. We'll use the CA
created in the previous stage to sign the server certificate for demonstration, but in real usage you may instead get 
it signed by an existing internal CA in your environment or a public CA.

The high-level steps involved in this stage are:

1. Generate a set of server certificate and private key.
2. Generate a Certificate Signing Request (CSR) for the server certificate.
3. Submit the CSR to a CA and obtain a signed certificate
4. Prepare a keystore containing the signed server certificate and the CA's certificate.
5. Export the server certificate's private key

**Note:** For services running on the same host, the same certificate can be used, if those services are 
accessed using the same hostname/IP address. Also, multi-server wildcard certificates can be used to share certificates across 
hosts to reuse certificates. However, it is strongly recommended that separate certificates be used for each service.

The steps are:

1. Generate a public/private key-pair and a X.509 certificate.

   ```bash
   $ keytool -storetype JKS -keystore <java-keystore-file-name> -storepass <java-keystore-password>\
           -genkey -keyalg <key-algorithm> -keysize <key-size> -keypass <private-key-password>\
           -alias <alias> -validity <validity>\
           -dname "<distinguished-name>"\
           -ext SAN=dns:<hostname1>,dns:<hostname2>,ip:<ipaddress1>
   ```
   
   Example:
   ```bash
   $ keytool -storetype JKS -keystore server.keystore.jks -storepass changeit\
              -genkey -keyalg RSA -keysize 2048 -keypass changeit\
              -alias server -validity 365\
              -dname "CN=server.pravega.io, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown"\
              -ext SAN=dns:server.abc.com,ip:127.0.0.1
   
   # Optionally, verify the contents of the generated file
   $ keytool -list -v -storetype JKS -keystore server.keystore.jks -storepass changeit
   ```

2. Generate a certificate signing request (CSR).

   It helps to think of a CSR as an application for getting a certificate signed by a trusted authority.

   A CSR is typically generated on the same server/node that hosts the TLS enabled service, so that the generated private key
   doesn't need to be moved anywhere else. In some other environments, CSRs are generated in a central server and the 
   resulting certificates are distributed to the services that need them.  

   ```bash
   $ keytool -keystore <java-keystore-file-path> -storepass <java-keystore-password> -alias <alias> \
             -certreq -file <csr-file-path> \
             -storepass <java-keystore-password>
   ```
   
   Example:
   ```bash
   $ keytool -keystore server.keystore.jks -storepass changeit -alias server \ 
             -certreq -file server.csr \
             -ext SAN=dns:server.abc.com,ip:127.0.0.1
   ```

3. Submit the CSR to a CA and obtain a signed certificate.

   If you are using a public or internal CA service, follow that CA's process for submitting the CSR and obtaining
   a signed certificate. To use the custom CA generated using the steps mentioned [earlier](#stage-1-setting-up-a-certificate-authority-ca) or an internal CA
   certificate/key bundle, use the steps below:
   
   First create a `server-csr.conf` file with the following contents. Replace `alt_names` with the server's hostname/ 
   IP address that Pravega will use to access the server. If the same certificate is used in multiple servers, add 
   hostnames/IP addresses of all the servers. 
   
   ```
   [req]
   req_extensions = v3_req
   prompt = no
   
   [v3_req]
   subjectAltName = @alt_names
   
   [alt_names]
   DNS.1 = server.abc.com
   DNS.2 = server.pravega.io
   IP.1 = 127.0.0.1
   ```
   
   Now, use the CA to sign the certificate:

   ```bash
   $ openssl x509 -req -CA <ca-cert-file-path> -CAkey <ca-private-key-file-path> \ 
           -in <csr-file-path> -out <signed-server-cert-file-path> \
           -days <validity> -CAcreateserial -passin pass:<csr-file-password> \
           -extfile server-csr.conf -extensions v3_req
   ```
   Example:
   ```bash
   $ openssl x509 -req -CA ca-cert.crt -CAkey ca-key.key \
        -in server.csr -out server-cert.crt \
        -days 365 -CAcreateserial -passin pass:changeit \
        -extfile server-csr.conf -extensions v3_req
   
   # Optionally, check the contents of the signed certificate
   $ openssl x509 -in server-cert.crt -text -noout
   ```

4. Prepare a keystore containing the CA's certificate and the signed server certificate. 

   ```bash
   # Import the CA certificate into a new keystore file.
   $ keytool -keystore <server-jks-keystore-file-path> -alias <ca-alias> -noprompt \
                -import -file <ca-cert-file-path> -storepass <server-jks-keystore-password>
   
    # Import the signed server certificate into the keystore.
      $ keytool -keystore <server-jks-keystore-file-path> -alias <server-alias> -noprompt \
                -import -file <signed-server-cert-file-path> -storepass <server-jks-keystore-password>
   ```
   
   Example:
   ```bash
   $ keytool -keystore server.keystore.jks -alias CARoot -noprompt \
             -import -file ca-cert.crt -storepass changeit

   $ keytool -keystore server.keystore.jks -alias server -noprompt \
                -import -file server-cert.crt -storepass changeit
   
   $ keytool -list -v -storepass changeit -keystore server.keystore.jks
   ```

5. Export the server's key into a separate file.

   This is a two-step process.
   * First, convert the server's keystore in `.jks` format into `.p12` format.

     ```bash
     $ keytool -importkeystore \
               -srckeystore <server-jks-keystore-file-path> \
               -destkeystore <server-pkcs12-keystore-file-path> \
               -srcstoretype jks -deststoretype pkcs12 \
               -srcstorepass <server-jks-keystore-password> -deststorepass <server-pkcs12-keystore>
     ```
     
     Example:
     ```bash
     $ keytool -importkeystore \
               -srckeystore server.keystore.jks \
               -destkeystore server.keystore.p12 \
               -srcstoretype jks -deststoretype pkcs12 \
               -srcstorepass changeit -deststorepass changeit
     ```
     
   * Now, export the private key of the server into a `PEM` encoded file. Note that the generated `PEM` file is not protected
   by a password. The key itself is password-protected, as we are using the `-nodes` flag. So, be sure
   to protect it using operating system's technical and procedural controls.

     ```bash
     $ openssl pkcs12 -in <server-pkcs12-keystore-file-path> -passin pass:<key-password> \
                      -nodes -out <server-key-file-path>
     ```
     
     Example:
     ```bash
     $ openssl pkcs12 -in server.keystore.p12 -out server-key.key -passin pass:changeit -nodes
     ```

Step 5 concludes this stage, and the stage is now set for installing the certificates and other PKI material in Pravega.

The table below lists the key output of this stage. Note that you'll typically need one of each file per Pravega
service, but you may share the same file for services collocated on the same host for logistical or economical reasons.

| File | Description| Command for Inspecting the Contents |
|:-----:| :---:|:--------|
| `server-cert.crt` | PEM-encoded CA-signed server certificate file | `openssl x509 -in server-cert.crt -text -noout`|
| `server-key.key` | PEM-encoded file containing the server's encrypted private key |  `openssl pkcs8 -inform PEM -in server-key.key -topk8` |
| `server.keystore.jks` | The server keystore file in .jks format | `keytool -list -v -keystore server.keystore.jks -storepass 1111_aaaa` |