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

import java.nio.file.WatchEvent;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import io.netty.handler.ssl.SslContext;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link Consumer} that acts upon modification of Segment Store SSL/TLS certificate.
 *
 * It creates a new {@link SslContext} and sets it in the constructor injected {@code sslContext}.
 */
@RequiredArgsConstructor
@Slf4j
public class TLSConfigChangeEventConsumer implements Consumer<WatchEvent<?>> {

    /**
     * A counter representing the number of times this object has been asked to
     * consume an event.
     */
    private final AtomicInteger numOfConfigChangesSinceStart = new AtomicInteger(0);

    private @NonNull final AtomicReference<SslContext> sslContext;
    private @NonNull final String pathToCertificateFile;
    private @NonNull final String pathToKeyFile;

    @Override
    public void accept(WatchEvent<?> watchEvent) {
        if (watchEvent != null) {
            log.info("Invoked for [{}]", watchEvent.context());
        } else {
            log.warn("Invoked for null watchEvent");
        }
        handleTlsConfigChange();
    }

    private void handleTlsConfigChange() {
        log.info("Current reload count = {}", numOfConfigChangesSinceStart.incrementAndGet());
        sslContext.set(TLSHelper.newServerSslContext(pathToCertificateFile, pathToKeyFile));
    }

    @VisibleForTesting
    int getNumOfConfigChangesSinceStart() {
        return numOfConfigChangesSinceStart.get();
    }
}
