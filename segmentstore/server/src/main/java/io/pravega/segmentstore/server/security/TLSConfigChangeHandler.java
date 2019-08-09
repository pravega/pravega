/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.security;

import io.netty.handler.ssl.SslContext;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@RequiredArgsConstructor
@Slf4j
public class TLSConfigChangeHandler {

    /**
     * A counter representing the number of times this object has been asked to
     * consume an event.
     */
    private final AtomicInteger numOfConfigChangesSinceStart = new AtomicInteger(0);

    private @NonNull final AtomicReference<SslContext> sslContext;
    private @NonNull final String pathToCertificateFile;
    private @NonNull final String pathToKeyFile;

    public void handleTlsConfigChange() {
        log.info("Current reload count = {}", numOfConfigChangesSinceStart.incrementAndGet());
        sslContext.set(TLSHelper.newServerSslContext(pathToCertificateFile, pathToKeyFile));
    }

    AtomicInteger getNumOfConfigChangesSinceStart() {
        return this.numOfConfigChangesSinceStart;
    }
}
