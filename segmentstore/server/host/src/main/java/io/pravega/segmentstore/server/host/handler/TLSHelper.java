/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.handler;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.pravega.common.Exceptions;

import java.io.File;
import javax.net.ssl.SSLException;

public class TLSHelper {

    static final String TLS_HANDLER_NAME = "tls";

    /**
     * Returns either null or a new instance of {@link SslContext} based on the specified arguments. A null is returned
     * if argument {@code sslEnabled} is null.
     *
     * @param isTlsEnabled whether SSL/TLS is enabled.
     * @param pathToCertificateFile the path to the PEM-encoded server certificate file
     * @param pathToServerKeyFile the path to the PEM-encoded file containing the server's encrypted private key
     * @return either null or a new instance of {@link SslContext} built from the specified
     *         {@code pathToCertificateFile} and {@code pathToServerKeyFile}
     * @throws NullPointerException if either {@code pathToCertificateFile} or {@code pathToServerKeyFile} is null
     * @throws IllegalArgumentException if either {@code pathToCertificateFile} or {@code pathToServerKeyFile} is empty
     * @throws SSLException if there is a failure in building the {@link SslContext}
     */
    static SslContext serverSslContext(boolean isTlsEnabled, String pathToCertificateFile, String pathToServerKeyFile)
            throws SSLException {
        if (isTlsEnabled) {
            return createServerSslContext(pathToCertificateFile, pathToServerKeyFile);
        } else {
            return null;
        }
    }

    /**
     * Creates a new instance of {@link SslContext}.
     *
     * @param pathToCertificateFile the path to the PEM-encoded server certificate file
     * @param pathToServerKeyFile the path to the PEM-encoded file containing the server's encrypted private key
     * @return a {@link SslContext} built from the specified {@code pathToCertificateFile} and {@code pathToServerKeyFile}
     * @throws NullPointerException if either {@code pathToCertificateFile} or {@code pathToServerKeyFile} is null
     * @throws IllegalArgumentException if either {@code pathToCertificateFile} or {@code pathToServerKeyFile} is empty
     * @throws SSLException if there is a failure in building the {@link SslContext}
     */
    static SslContext createServerSslContext(String pathToCertificateFile, String pathToServerKeyFile)
            throws SSLException {
        Exceptions.checkNotNullOrEmpty(pathToCertificateFile, "pathToCertificateFile");
        Exceptions.checkNotNullOrEmpty(pathToServerKeyFile, "pathToServerKeyFile");
        return SslContextBuilder.forServer(new File(pathToCertificateFile), new File(pathToServerKeyFile)).build();
    }
}
