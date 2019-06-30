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

import java.nio.file.WatchEvent;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link Consumer} that acts upon change in Segment Store SSL/TLS configuration change.
 */
@RequiredArgsConstructor
@Slf4j
public class TLSConfigChangeEventConsumer implements Consumer<WatchEvent<?>> {

    /**
     * A counter representing the number of times this object has been asked to
     * consume an event.
     */
    private final AtomicInteger numOfConfigChangesSinceStart = new AtomicInteger(0);

    private @NonNull final Channels channels;

    @Override
    public void accept(WatchEvent<?> watchEvent) {
        log.debug("Invoked for [{}]", watchEvent.context());
        handleTlsConfigChange();
    }

    private void handleTlsConfigChange() {
        log.info("Current reload count = {}", numOfConfigChangesSinceStart.incrementAndGet());
        channels.flushStopAndRefresh();
    }
}
