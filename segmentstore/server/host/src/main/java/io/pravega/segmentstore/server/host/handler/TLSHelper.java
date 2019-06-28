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
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import javax.net.ssl.SSLException;

@Slf4j
public class TLSHelper {

    static final String TLS_HANDLER_NAME = "tls";

    /**
     * Creates a new instance of {@link SslContext}.
     *
     * @param pathToCertificateFile the path to the PEM-encoded server certificate file
     * @param pathToServerKeyFile the path to the PEM-encoded file containing the server's encrypted private key
     * @return a {@link SslContext} built from the specified {@code pathToCertificateFile} and {@code pathToServerKeyFile}
     * @throws NullPointerException if either {@code pathToCertificateFile} or {@code pathToServerKeyFile} is null
     * @throws IllegalArgumentException if either {@code pathToCertificateFile} or {@code pathToServerKeyFile} is empty
     * @throws RuntimeException if there is a failure in building the {@link SslContext}
     */
    static SslContext createServerSslContext(String pathToCertificateFile, String pathToServerKeyFile) {
        Exceptions.checkNotNullOrEmpty(pathToCertificateFile, "pathToCertificateFile");
        Exceptions.checkNotNullOrEmpty(pathToServerKeyFile, "pathToServerKeyFile");
        SslContext result = null;
        try {
            result = SslContextBuilder.forServer(new File(pathToCertificateFile), new File(pathToServerKeyFile)).build();
            log.debug("Done creating a new SSL Context for the server.");
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}
