<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Running Pravega

Running in local machine allows us to get started using Pravega very quickly. Most of the options uses the standalone mode which is suitable for most of the development and testing.

The prerequisites for running in local machine is described below.

## Standalone Mode

### From Source

First we need to have the Pravega source code checked out to download the dependencies:

```
git clone https://github.com/pravega/pravega.git
cd pravega
```

As a next step compile Pravega and start the standalone deployment.

```
./gradlew startStandalone
```

### From Installation Package

Download the Pravega latest release from the [Github Releases](https://github.com/pravega/pravega/releases). The tarball or zip files can be used as they are identical. Instructions are provided for the tar files, but the same can be used for the zip file also.


```
tar xfvz pravega-0.1.0.tgz
```

Run Pravega standalone:

```
pravega-0.1.0/bin/pravega-standalone
```

### From Docker Container

The below command will download and run Pravega from the container image on docker hub.

**Note:** We must replace the `<ip>` with the IP of our machine to connect to Pravega from our local machine. Optionally we can replace `latest` with the version of Pravega as per the requirement.


```
docker run -it -e HOST_IP=<ip> -p 9090:9090 -p 12345:12345 pravega/pravega:latest standalone
```

## Docker Compose (Distributed Mode)

Unlike other options for running locally, the docker compose option runs a full Pravega install in distributed mode. It contains containers for running Zookeeper, Bookkeeper and HDFS. Hence Pravega operates as if it would in production. This is the easiest way to get started with the standalone option but requires additional resources.


To use this we need to have Docker `1.12` or later versions.

Download the [docker-compose.yml](https://github.com/pravega/pravega/tree/master/docker/compose/docker-compose.yml) from github. For example:

```
wget https://raw.githubusercontent.com/pravega/pravega/master/docker/compose/docker-compose.yml
```

We need to set the IP address of our local machine as the value of HOST_IP in the following command.
```
HOST_IP=1.2.3.4 docker-compose up
```
Clients can then connect to the controller at `${HOST_IP}:9090`.

## Configuring standalone

Configure standalone server to communicate using SSL/TLS. To do so, edit the TLS-related properties in `standalone-config.properties` as shown below:

  ```java
  singlenode.enableTls=true
  singlenode.keyFile=../config/key.pem
  singlenode.certFile=../config/cert.pem
  singlenode.keyStoreJKS=../config/standalone.keystore.jks
  singlenode.keyStoreJKSPasswordFile=../config/standalone.keystore.jks.passwd
  singlenode.trustStoreJKS=../config/standalone.truststore.jks

  ```
These properties include ports for Zookeeper, Segment Store and Controller. They also contain other configurations related to security.

## Running standalone with encryption and authentication enabled

The configurations, `singlenode.enableTls` and `singlenode.enableauth` can be used to enable encryption and authentication respectively.(By default both these settings are disabled).

In case `enableTls` is set to true, the default certificates provided in the `conf` directory are used for setting up TLS. These can be overridden by specifying in the properties file.

1. Ensure that the server's certificate is trusted. If you run `/gradlew startStandalone` without it, you'll encounter the following error:

```java
Caused by: sun.security.validator.ValidatorException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target
        at sun.security.validator.PKIXValidator.doBuild(PKIXValidator.java:397)
        at sun.security.validator.PKIXValidator.engineValidate(PKIXValidator.java:302)
        at sun.security.validator.Validator.validate(Validator.java:260)
        at sun.security.ssl.X509TrustManagerImpl.validate(X509TrustManager
```
2. To ensure the server's certificate is trusted, import it into the JVM's truststore. The following command sequence is used (in Linux) with the provided certificate file "cert.pem":

 - `cd /path/to/pravega/config`
 - Convert the 'cert.pem' file to `DER` format: `openssl x509 -in cert.pem -inform pem -out cert.der -outform der`
 - Import the certificate into the local JVM's trust store:
  `sudo keytool -importcert -alias local-CA -keystore /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/security/cacerts  -file cert.der` (using the default password 'changeit')

3. Run Pravega standalone mode using command `./gradlew startStandalone`.

4. Verify that controller REST API is returning response over SSL/TLS:

    ```java
     curl -v -k https://104.215.152.115:9091/v1/scopes
    ```
    `-v` is to avoid hostname verification, since we are using the provided certificate
    which isn't assigned to your hostname. You can find details about curl's options [here](https://curl.haxx.se/docs/manpage.html).

5.  Run [Reader/Writer](https://github.com/pravega/pravega-samples/blob/master/pravega-client-examples/README.md) Pravega sample application against the standalone server to verify it is responding appropriately to `Read/Write` requests. To do so, in the `ClientConfig`, set the following:

    ```java
    ClientConfig clientConfig = ClientConfig.builder()
                 .controllerURI(...)
                 .trustStore("/path/to/cert.pem")
                 .validateHostName(false)
                 .build();
    ```
    Everything else should be the same as other reader/writer apps.


6. Clients can then connect to the controller at `${HOST_IP}:9090`.
