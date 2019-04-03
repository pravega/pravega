<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Running Pravega


Running in local machine allows us to get started using Pravega very quickly. Running Pravega in Standalone mode is suitable for development and testing Pravega applications.

The prerequisites for running in local machine is described below.

## Standalone Mode

### From Source

Checkout the source code:

```
git clone https://github.com/pravega/pravega.git
cd pravega
```

Build the Pravega standalone distribution:

```
./gradlew startStandalone
```

### From Installation Package

Download the Pravega release from the [GitHub Releases](https://github.com/pravega/pravega/releases).

```
$ tar xfvz pravega-<version>.tgz

```
Download and extract either tarball or zip files. Follow the instructions provided for the tar files (same can be applied for zip file) to launch all the components of Pravega on your local machine.

Run Pravega Standalone:

```
pravega-<version>/bin/pravega-standalone

```

### From Docker Container

The below command will download and run Pravega from the container image on docker hub.

**Note:** We must replace the `<ip>` with the IP of our machine to connect to Pravega from our local machine. Optionally we can replace `latest` with the version of Pravega as per the requirement.

```
docker run -it -e HOST_IP=<ip> -p 9090:9090 -p 12345:12345 pravega/pravega:latest standalone
```

## Docker Compose (Distributed Mode)

Unlike other options for running locally, the docker compose option runs a full deployment of Pravega
in distributed mode. It contains containers for running Zookeeper, Bookkeeper and HDFS. Hence Pravega operates as if it would in production. This is the easiest way to get started with the standalone option but requires additional resources.

**Prerequisite:** Docker `1.12` or later.

Download the [docker-compose.yml](https://github.com/pravega/pravega/tree/master/docker/compose/docker-compose.yml) from github. For example:

```
wget https://raw.githubusercontent.com/pravega/pravega/master/docker/compose/docker-compose.yml
```

We need to set the IP address of our local machine as the value of `HOST_IP` in the following command.

```
HOST_IP=1.2.3.4 docker-compose up
```
Clients can then connect to the controller at `${HOST_IP}:9090`.

## Running Pravega in Standalone Mode with SSL/TLS Enabled

SSL/TLS `singlenode.enableTls` is disabled by default in Pravega standalone mode cluster. The following steps explains how to enable and run standalone mode cluster with SSL/TLS enabled:

1. Configure standalone server to communicate using SSL/TLS. To do so, edit the TLS-related properties in `standalone-config.properties` as shown below:

     ```java
        singlenode.enableTls=true
        singlenode.keyFile=../config/key.pem
        singlenode.certFile=../config/cert.pem
        singlenode.keyStoreJKS=../config/standalone.keystore.jks
        singlenode.keyStoreJKSPasswordFile=../config/standalone.keystore.jks.passwd
        singlenode.trustStoreJKS=../config/standalone.truststore.jks

     ```

2. Ensure that the server's certificate is trusted. To ensure the server's certificate is trusted, import it into the JVM's truststore. The server certificate used for the Pravega standalone mode server must be trusted on the JVM that runs the server. A server certificate can be rendered trusted via either Chain of Trust or via Direct Trust.

    **Chain of Trust:** A chain of trust, which is the standard SSL/TLS certificate trust model, is established by verifying that the certificate is issued and signed by a trusted CA. If you are using a certificate issued by a CA as the server certificate, ensure that the CA's certificate is in the JVM's truststore.

    **Direct Trust:** This type of trust is established by adding the server's certificate to the truststore. It is a non-standard way of establishing trust when using self-signed certificates. If you using self-signed certificates such as those provided by Pravega for standalone mode, ensure that the server's certificate is in the JVM's truststore.

The following command sequence is used (in Linux) with the provided certificate file `cert.pem` into the JVM's system truststore.

   - `cd /path/to/pravega/config`
   - Convert the `cert.pem` file to `DER` format: `openssl x509 -in cert.pem -inform pem -out cert.der  -outform der`
   - Import the certificate into the local JVM's trust store:
    `sudo keytool -importcert -alias local-CA -keystore /path/to/jre/lib/security/cacerts  -file cert.der` (using the default password `changeit`)

   **Note:** If you want to use a custom truststore instead of adding the certificate to the system truststore, create a new truststore using Java keytool utility, add the certificate to it and configure the JVM to use it by setting the system properties `javax.net.ssl.trustStore` and `javax.net.ssl.trustStorePassword`.

3. Run Pravega standalone mode using command `./gradlew startStandalone`.

4. Verify that controller REST API is returning response over SSL/TLS:

    ```java
      curl -v -k https://<host-name>/v1/scopes
    ```
    `-v` is to avoid hostname verification, since we are using the provided certificate which isn't assigned to your hostname. You can find details about curl's options [here](https://curl.haxx.se/docs/manpage.html).

5. Run Reader/Writer [Pravega sample applications](https://github.com/pravega/pravega-samples/blob/master/pravega-client-examples/README.md) against the standalone server to verify it is responding appropriately to `Read/Write` requests. To do so, in the `ClientConfig`, set the following:

   ```java
    ClientConfig clientConfig = ClientConfig.builder()
                 .controllerURI(...)
                 .trustStore("/path/to/cert.pem")
                 .validateHostName(false)
                 .build();
   ```
6. Everything else should be the same as the Reader/Writer apps.
