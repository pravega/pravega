<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

# Setting Up Security for a Standalone Mode Cluster

Security in Pravega is off by default. You may start Pravega Standalone mode with security enabled. When security is enabled, either or both of the following occurs:

1. Client-server and internal communications are encrypted using SSL/TLS
2. Server authenticates and authorizes client requests

For standalone mode clusters, you may enable SSL/TLS, and/ `auth` (short for Authentication and Authorization). We recommend that you enable both. The subsections below describe how to enable them.

## Enabling SSL/TLS

The configuration parameter `singlenode.enableTls` determines whether SSL/TLS is enabled in a standalone mode cluster. It's default value is `false`, and therefore, SSL/TLS is disabled by default as well.

The following steps explain how to enable and configure SSL/TLS for a standalone mode configuration:

1. Locate the `standalone-config.properties` file.

   If you are running standalone mode cluster from source, you can find it under the `/path/to/pravega/config` directory.

   If you are running the cluster from the distribution, you can find it under the `/path/to/pravega-<version>/conf` directory.

2. Optionally, prepare the certificates and keys for SSL/TLS.

   In the same directory as the `standalone-config.properties` file, you'll find sample PKI material. You may use these or create your own.

   If you'd like to create your own material instead, you may follow similar steps as [those](https://github.com/pravega/pravega/wiki/Creating-TLS-material-for-Pravega-security) used for creating the supplied material.

2. Configure SSL/TLS properties in `standalone-config.properties` as shown below.

   If you are using custom material, replace the paths of the files accordingly.

   ```java
   singlenode.enableTls=true
   singlenode.keyFile=../config/server-key.key
   singlenode.certFile=../config/server-cert.crt
   singlenode.keyStoreJKS=../config/server.keystore.jks
   singlenode.keyStoreJKSPasswordFile=../config/server.keystore.jks.passwd
   singlenode.trustStoreJKS=../config/client.truststore.jks
   ```

  For descriptions of these properties, see [Pravega Security Configurations](security-configurations.md).  

2. Ensure that the server's certificate is trusted.

   Note: This step is only needed the first time you run SSL/TLS-enabled standalone mode with a certificate.If you have already imported the certificate earlier, you don't need to do it again the next time you launch the server.

   A server certificate can be rendered trusted via either Chain of Trust or via Direct Trust.

   * **Chain of Trust:** A chain of trust, which is the standard SSL/TLS certificate
     trust model, is established by verifying that the certificate is issued and signed by a trusted CA. To establish chain of trust for a certificate signed by a custom CA, we need to import the certificate of the CA into the JVM's truststore. The provided `server-cert.crt` is signed using a CA represented by another provided certificate `ca-cert.crt`: so, when using those certificate, you should import `ca-cert.crt` file.

  * **Direct Trust:** This type of trust is established by adding the server's
      certificate to the truststore. It is a non-standard way of establishing trust, which is useful when using self-signed certificates.

   Here's how to import the provided CA certificate file `ca-cert.crt` to Java's system truststore, for establishing 'Chain of Trust':

   a) Change directory to Standalone mode config directory.

      ```bash
      $ cd /path/to/pravega/config
      ```
      or
      ```bash
      $ cd /path/to/pravega-<version>/conf
      ```

   b) Convert the X.509 PEM (ASCII) encoded `ca-cert.crt` file to DER (Binary) encoded format:

      ```bash
      $ openssl x509 -in server-cert.crt -inform pem -out ca-cert.der -outform der
      ```

   c) Import the certificate into the local JVM's trust store:

      ```bash
      $ sudo keytool -importcert -alias local-CA \
           -keystore /path/to/jre/lib/security/cacerts  -file ca-cert.der
      ```
      (using the default password `changeit`)

   **Note:** If you want to use a custom truststore instead of adding the certificate to the system truststore, create a new truststore using Java keytool utility, add the certificate to it and configure the JVM to use it by setting the system properties `javax.net.ssl.trustStore` and `javax.net.ssl.trustStorePassword`.

3. Run Pravega standalone mode.

   If you are running it from the source code, use

   ```bash
   $ ./gradlew startStandalone
   ```

   If you are running it from the distribution, use

   ```bash
   $ /path/to/pravega-<version>/bin/pravega-standalone
   ```

4. Verify that controller REST API is returning response over SSL/TLS:

   ```bash
   $ curl -v -k https://<host-name-or-ip>:9091/v1/scopes
   ```

    `-k` in the command above avoids hostname verification. If you are using the provided certificate, the host's DNS name/IP isn't the subject (in `CN` or `SAN`), and therefore hostname verification will fail. `-k` lets the command to return data successfully. You can find details about curl's options [here](https://curl.haxx.se/docs/manpage.html).

5. Run Reader/Writer [Pravega sample applications](https://github.com/pravega/pravega-samples/blob/master/pravega-client-examples/README.md) against the standalone server to verify it is responding appropriately to `Read/Write` requests. To do so, in the `ClientConfig`, set the following:

   ```java
   ClientConfig clientConfig = ClientConfig.builder()
                 .controllerURI("tls://localhost:9090")
                 .trustStore("/path/to/ca-cert.crt")
                 .validateHostName(false)
                 .build();
   ```
   **Note:**
   * Remember that clients can access standalone mode clusters through the localhost interface only. Therefore, the hostname in the Controller URI should be specified as `localhost` in client applications when accessing standalone mode cluster.
   * `.validateHostName(false)` disables hostname verification for client-to-segment-store communications.

  Everything else should be the same as the Reader/Writer apps.
