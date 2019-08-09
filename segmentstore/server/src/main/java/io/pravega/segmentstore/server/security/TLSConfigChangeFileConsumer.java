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

import com.google.common.annotations.VisibleForTesting;
import io.netty.handler.ssl.SslContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@RequiredArgsConstructor
@Slf4j
public class TLSConfigChangeFileConsumer implements Consumer<File> {

    private final TLSConfigChangeHandler handler;

    public TLSConfigChangeFileConsumer(AtomicReference<SslContext> sslContext, String pathToCertificateFile,
                                       String pathToKeyFile) {
        handler = new TLSConfigChangeHandler(sslContext, pathToCertificateFile, pathToKeyFile);
    }

    @Override
    public void accept(File file) {
        log.debug("Invoked for file [{}] ", file.getPath());
        handler.handleTlsConfigChange();
    }

    @VisibleForTesting
    int getNumOfConfigChangesSinceStart() {
        return handler.getNumOfConfigChangesSinceStart().get();
    }
}
