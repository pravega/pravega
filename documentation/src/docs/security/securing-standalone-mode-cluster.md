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

# Setting Up Security for a Standalone Mode Server

Security in Pravega is off by default. You may start Pravega Standalone mode server with security enabled by modifying the security configuration before launching it.

Depending on how you configure security, either or both of the following occurs:

1. Client-server and internal communications are encrypted using SSL/TLS
2. Server authenticates and authorizes client requests

For standalone mode servers, you may enable SSL/TLS, and/ `auth` (short for Authentication and Authorization). We recommend that you enable both. The subsections below describe how to enable them.

## Enabling SSL/TLS and Auth

The configuration parameter `singlenode.security.tls.enable` determines whether SSL/TLS is enabled in a standalone mode server. Its default value is `false`, and therefore, SSL/TLS is disabled by default.

The configuration parameter `singlenode.security.tls.protocolVersion` configures the TLS Protocol Version. Its default value is `TLSv1.2,TLSv1.3` which is a mixed mode supporting both `TLSv1.2` and `TLSv1.3`. Pravega also supports strict `TLSv1.2` and strict `TLSv1.3` modes.

Similarly, the configuration parameter `singlenode.security.auth.enable` determines whether `auth` is enabled. It is disabled by default as well.

The following steps explain how to enable and configure SSL/TLS and/ `auth`:

1. Locate the `standalone-config.properties` file.

   If you are running standalone mode server from source, you can find it under the `/path/to/pravega/config` directory.

   If you are running it from the distribution instead, you can find it under the `/path/to/pravega-<version>/conf` directory.

2. If enabling SSL/TLS, optionally prepare the certificates and keys for SSL/TLS.

   In the same directory as the `standalone-config.properties` file, you'll find default/provided PKI material. You may use these material for SSL/TLS.

   If you'd like to create your own material instead, you may follow similar steps as [those](https://github.com/pravega/pravega/wiki/Creating-TLS-material-for-Pravega-security) used for creating the supplied material.

2. If enabling SSL/TLS, configure SSL/TLS parameters.

   If you are using custom material, replace the paths of the files accordingly.

   ```java
   singlenode.security.tls.enable=true
   singlenode.security.tls.protocolVersion=TLSv1.2,TLSv1.3
   singlenode.security.tls.privateKey.location=../config/server-key.key
   singlenode.security.tls.certificate.location=../config/server-cert.crt
   singlenode.security.tls.keyStore.location=../config/server.keystore.jks
   singlenode.security.tls.keyStore.pwd.location=../config/server.keystore.jks.passwd
   singlenode.security.tls.trustStore.location=../config/client.truststore.jks
   ```
   For descriptions of these properties, see [Pravega Security Configurations](./pravega-security-configurations.md).  

3. If enabling `auth`, configure `auth` parameters.

   ```java
   singlenode.security.auth.enable=true
   singlenode.security.auth.credentials.username=admin
   singlenode.security.auth.credentials.pwd=1111_aaaa
   singlenode.security.auth.pwdAuthHandler.accountsDb.location=../config/passwd
   ```
   For descriptions of these properties, see [Pravega Security Configurations](./pravega-security-configurations.md).

   Note:
   * The default Password Auth Handler supports `Basic` authentication for client-server communications. You may deploy additional Auth Handler plugins by placing them in the classpath.

   * The default handler's configuration file shown above - `passwd` file - contains a single user with credentials containing username `admin` and password `1111_aaaa`.

   * To create additional users or change the password for the default `admin` user:

     a) Create a file containing 1 or more entries of the form  `<user>:<password>:<acl>;`.

        For example: `jdoe:strong-password:*,READ_UPDATE;`

     b) Use the PasswordCreatorTool to create a new file with the passwords encrypted. Configure this new file instead of the supplied `passwd` file.

4. If enabling SSL/TLS, ensure that the server's certificate is trusted.

   Note: This step is only needed the first time you run SSL/TLS-enabled standalone mode with a certificate. If you have already imported the certificate earlier, you don't need to do it again the next time you launch the server.

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
      When prompted for keystore password, use the default JRE keystore password `changeit` or the custom one that you might have configured for the JRE.

   Note: If you want to use a custom truststore instead of adding the certificate to the system truststore, create a new truststore using Java keytool utility, add the certificate to it and configure the JVM to use it by setting the system properties `javax.net.ssl.trustStore` and `javax.net.ssl.trustStorePassword`.

Now that you have enabled and configured security, restart the standalone mode server and [verify](#verifying-security) that security is working.

## Verifying Security

1. Verify that the controller REST API is responding properly:

   ```bash
   # If both TLS and Auth are enabled
   $ curl -v -k -u admin:1111_aaaa https://<host-name-or-ip/localhost>:9091/v1/scopes

   # If only Auth is enabled
   $ curl -v -u admin:1111_aaaa http://<host-name-or-ip/localhost>:9091/v1/scopes
   ```

   Note: `-k` in the command above avoids hostname verification and is needed only if you are enabling SSL/TLS. If you are using the provided certificate, the host's DNS name/IP isn't the subject (in `CN` or `SAN`), and therefore hostname verification will fail. `-k` lets the command to return data successfully. You can find details about curl's options [here](https://curl.haxx.se/docs/manpage.html).

2. Optionally, run security-enabled Reader/Writer from [Pravega sample applications](https://github.com/pravega/pravega-samples/blob/master/pravega-client-examples/README.md) against the standalone server to verify it is responding appropriately to `Read/Write` requests.

   To do so, configure security in the client application and run it. The example below assumes both TLS and `auth` are enabled. If TLS is disabled, modify the Controller URI scheme to `tcp` instead of `tls` and remove the lines that add
   TLS-related client-side configuration.

   ```java
    ClientConfig clientConfig = ClientConfig.builder()
                 .controllerURI(URI.create("tls://localhost:9090"))

                  // TLS-related client-side configuration
                 .trustStore("/path/to/ca-cert.crt")
                 .validateHostName(false)

                  // Auth-related client-side configuration
                 .credentials(new DefaultCredentials(this.password, this.username))

                 .build();
   ```

  You can find a client example with security enabled [here](https://github.com/pravega/pravega-samples/tree/master/pravega-client-examples/src/main/java/io/pravega/example/secure).

  Note:
  * Remember that clients can access standalone mode servers through the localhost interface only. Therefore, the hostname in the Controller URI should be specified as `localhost` in client applications when accessing standalone mode servers.
  * `.validateHostName(false)` disables hostname verification for client-to-segment-store communications.
