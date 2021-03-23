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
# Creating TLS Certificates, Keys and Other TLS Artifacts

This document describes how to create TLS certificates, keys and other artifacts for the server components. The 
generated CA certificate and Java truststore may also be used on client-side for validating the servers' certificates.

   * [Overview](#overview)
   * [Setting up a Certificate Authority (CA)](#setting-up-a-certificate-authority-ca)
   * [Generate TLS Certificates and Other TLS Artifacts for the Server](#generate-tls-certificates-and-other-tls-artifacts-for-the-server)

## Overview

A TLS server certificate is an X.509 digital certificate that binds the server's identity (such as the server's 
DNS name or IP address or both) to the server's public key. The public key is part of the public-private key pair used 
for asymmetric encryption during the TLS handshake. 

Each Controller and Segment Store service requires a pair of PEM-encoded TLS certificate and private key. Each 
service also requires a PEM-encoded certificate of the CA that has signed the server certificates. Moreover, the 
Controller REST interface requires its certificate and keys in Java JKS formats. We refer to all the required files 
as TLS artifacts, henceforth. 

While the TLS certificate is public, the corresponding key is private and must be kept protected (typically
in the server that uses the TLS certificate and the key). Each server should use its own set of TLS artifacts; 
reusing certificates requires reusing private keys as well, and sharing private keys across multiple servers increases 
the risk that they will be compromised.

Here are a few strategies for determining the sets of TLS artifacts required for a Pravega cluster:

1. _Use separate TLS artifacts for each host/machine or service_. 
   
   Manual deployments supports this and the other configurations listed below. This is the most secure option. 
2. _Use two sets of TLS artifacts - one for the Controllers and the other for the Segment Stores_. 
   
   In this case, each certificate is assigned to all the respective nodes (Controllers or Segment Stores) by specifying 
   the nodes' DNS names and/or IP addresses in the certificate's 
   [Subject Alternative Name (SAN)](https://en.wikipedia.org/wiki/Subject_Alternative_Name) field. Also, the server 
   certificates might need to be recreated to add DNS names and/or IP addresses of the new servers whenever the 
   Controllers or the Segment stores are scaled up.
   
   Kubernetes and other forms of containerized deployments supports this configuration. 

3. _Use the same set for all Controllers and Segment Store services_. 
   
   In this case, the certificate must contain the DNS names and/or IP addresses of all the Controller and Segment Store 
   services. This is the least secure option. Also, the server certificate might need to be recreated to add DNS names 
   and/or IP addresses of the new servers whenever the Controllers or the Segment stores are scaled up.

   All deployment options support this configuration.
   
The chosen configuration determines the number of TLS artifacts sets required for a 
Pravega cluster. Each set can be prepared using the process described later in this document under
[Generate TLS Certificates and Other TLS Artifacts for the Server](#generate-tls-certificates-and-other-tls-artifacts-for-the-server). 

Another important consideration is whether you want to terminate TLS at an external infrastructural component like 
reverse proxy, Kubernetes Ingress or a load balancer. If TLS is terminated at such an infrastructural component, instead 
of Pravega services, you will need to generate TLS artifacts for those infrastructural components instead, using similar
steps described in this document; however, the exact steps may be slightly different and additional steps may be required to 
ensure that other parts of the infrastructure can communicate with it. Refer to the respective platform vendor's 
documentation, 
such as [this](https://cloud.google.com/kubernetes-engine/docs/concepts/ingress) one from Google Kubernetes Engine, 
for instructions.

**Before you Begin:**

The steps described in this document use OpenSSL and Java Keytool to generate TLS artifacts like certificates, keys, 
Java keystores and Java truststores. Install OpenSSL and Java Development Kit (JDK) on the hosts where the artifacts 
will be generated.

**Note:**
* The examples shown in this document use command line arguments to pass all inputs to the command. To pass
  sensitive command arguments via prompts instead, just exclude the corresponding option (as shown in the second command
  below). 

  ```bash
  # Passwords are passed as command line arguments.
  $ keytool -keystore server01.keystore.jks -alias server01 -validity <validity> -genkey \
              -storepass <keystore-password> -keypass <key-password> \
              -dname <distinguished-name> -ext SAN=DNS:<hostname>,

  # Passwords (and some other arguments) are to be entered interactively on the prompt.
  $ keytool -keystore server01.keystore.jks -alias server01 -genkey
  ```
* A weak password `changeit` is used everywhere in this document for easier reading. Be sure to replace it with 
  a strong and separate password for each file.

## Setting up a Certificate Authority (CA)

If you are planning to use a public CA or an existing private/internal CA, you may skip 
this part altogether and go directly to the [next section](#generate-tls-certificates-and-other-tls-artifacts-for-the-server).

Here, we'll create a CA in the form of a public/private key pair and a self-signed certificate.
Later, we'll use this CA certificate/key bundle to sign the servers' certificates.

1. Generate a CA certificate and public/private key pair. 

   ```bash
   $ openssl req -new -x509 -keyout <ca-private-key-file-path> -out <ca-cert-file-path> -days <validity-in-days> \
            -subj "<distinguished_name>" \
            -passout pass:<ca-private-key-password>
   ```

   Example:
   ```bash
   # In MinGW/MSYS, the same -subj argument will be:
   # "//C=US\ST=Washington\L=Seattle\O=Pravega\OU=CA\CN=Pravega-Stack-CA"
   $ openssl req -new -x509 -keyout ca-key.key -out ca-cert.crt -days 365 \
            -subj "/C=US/ST=Washington/L=Seattle/O=Pravega/OU=CA/CN=Pravega-Stack-CA" \
            -passout pass:changeit
   ```
  
   The command above will generate the following two PEM-encoded files: 
   * A file `ca-key.key` containing the encrypted private key.
   * A file `ca-cert.crt` containing the CA certificate.
   
2. Optionally, create a Java truststore containing the CA's certificate. This may be used by client applications if 
   they configure the truststore using the `javax.net.ssl.trustStore` Java option. 
   
   ```bash
   $ keytool -keystore <java-truststore-file-path> -noprompt \
             -alias <trusted-cert-entry-alias> \
             -import -file <ca-cert-file-path> \
             -storepass <java-truststore-file-password>
   ```
   
   Example:
   ```bash
   $ keytool -keystore client.truststore.jks -noprompt \
             -alias caroot \ 
             -import -file ca-cert.crt \
             -storepass changeit

   # Optionally, list the truststore's contents and inspect the output to verify everything is in order. The output 
   # should show a single entry with alias name `caroot` and entry type `trustedCertEntry`.
   $ keytool -list -v -keystore client.truststore.jks -storepass changeit
   ```

At this point, the following CA and truststore artifacts should be ready:

| File | Description | Example command for inspecting the file's Contents |
|:-----:|:--------|:--------------|
| `ca-cert.crt` | A PEM-encoded file containing the X.509 certificate of the CA | `$ openssl x509 -in ca-cert.crt -text -noout` |
| `ca-key.key` | A PEM-encoded file containing the CA's encrypted private key  | `$ openssl pkcs8 -inform PEM -in ca-key.key -topk8` |
| `client.truststore.jks` | A password-protected truststore file containing the CA's certificate | `$ keytool -list -v -keystore client.truststore.jks -storepass changeit` |

## Generate TLS Certificates and Other TLS Artifacts for the Server

Here, we prepare a set of TLS artifacts that can be used by one or more nodes.  

We'll use the CA created in the previous stage to sign the server certificate for demonstration, but in real usage you 
may instead get it signed by an existing internal CA in your environment or a public CA.

The high-level steps involved in this stage are:

1. Generate a set of server certificate and private key.
2. Generate a Certificate Signing Request (CSR) for the server certificate.
3. Submit the CSR to a CA and obtain a signed certificate.
4. Prepare a keystore containing the signed server certificate and the CA's certificate.
5. Export the server certificate's private key.

Note that these steps will need to be repeated with different input for each set. We discussed the number of sets 
required earlier in the [Overview](#overview) section. 

The above steps are described below: 

1. Generate a set of server certificate and private key.

   The following command will generate a Java JKS keystore containing the following artifacts for the server:
   * An X.509 certificate containing the public key identifying the server
   * A private key for the public/private key pair

   ```bash
   $ keytool -storetype JKS -keystore <java-keystore-file-name> -storepass <java-keystore-password>\
           -genkey -keyalg <key-algorithm> -keysize <key-size> -keypass <private-key-password>\
           -alias <alias> -validity <validity>\
           -dname "<distinguished-name>"\
           -ext SAN=dns:<hostname1>,dns:<hostname2>,ip:<ipaddress1>
   ```
   
   Example: 
   ```bash
   $ keytool -storetype JKS -keystore server_unsigned.keystore.jks -storepass changeit\
              -genkey -keyalg RSA -keysize 2048 -keypass changeit\
              -alias server -validity 365\
              -dname "CN=server.pravega.io, OU=analytics, O=pravega, L=Seattle, ST=Washington, C=US"\
              -ext SAN=dns:server1.pravega.io,dns:server2.pravega.io,ip:127.0.0.1
   
   # Optionally, verify the contents of the generated file
   $ keytool -list -v -storetype JKS -keystore server_unsigned.keystore.jks -storepass changeit
   ```

2. Generate a Certificate Signing Request (CSR) for the server certificate.

   It helps to think of a CSR as an application for getting a certificate signed by a trusted authority. 
   
   A CSR is typically generated on the same server/node that uses the certificate, so that the corresponding private key
   doesn't need to be moved anywhere else. In some other environments, CSRs are generated in a central server and the 
   resulting certificates are distributed to the services that need them.  

   ```bash
   $ keytool -keystore <java-keystore-file-path> -storepass <java-keystore-password> -alias <alias> \
             -certreq -file <csr-file-path> \
             -storepass <java-keystore-password>
   ```
   
   Example:
   ```bash
   $ keytool -keystore server_unsigned.keystore.jks -storepass changeit -alias server \ 
             -certreq -file server.csr \
             -ext SAN=dns:server1.pravega.io,dns:server2.pravega.io,ip:127.0.0.1
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
   DNS.1 = server1.pravega.io
   DNS.2 = server2.pravega.io
   IP.1 = 127.0.0.1
   ```
   
   Now, have the CA sign the certificate:

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
   
   # Optionally, check the contents of the signed certificate.
   $ openssl x509 -in server-cert.crt -text -noout
   ```

4. Prepare a new keystore containing the signed server certificate and the CA's certificate chain. 

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
   
   # Optionally, list the keystore and inspect the output
   $ keytool -list -v -storepass changeit -keystore server.keystore.jks
   ```

5. Export the server certificate's private key. 
   
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

Step 5 concludes this stage and we are now now set for installing the certificates and other TLS artifacts in Pravega.

The table below lists the key output of this stage. 

| File | Description| Command for Inspecting the Contents |
|:-----:| :---:|:--------|
| `server-cert.crt` | A PEM-encoded file containing the CA-signed X.509 server certificate | `openssl x509 -in server-cert.crt -text -noout`|
| `server-key.key` | A PEM-encoded file containing the server's encrypted private key |  `openssl pkcs8 -inform PEM -in server-key.key -topk8` |
| `server.keystore.jks` | A password-protected server keystore file in .jks format | `keytool -list -v -keystore server.keystore.jks -storepass changeit` |