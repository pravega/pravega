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

@RequiredArgsConstructor
@Slf4j
public class TLSConfigChangeEventConsumer implements Consumer<WatchEvent<?>> {

    /**
     * A counter representing the number of times this object has been asked to
     * consume an event.
     */
    private static final AtomicInteger NUM_OF_CONFIG_CHANGES_SINCE_START = new AtomicInteger(0);

    private @NonNull final Channels channels;

    @Override
    public void accept(WatchEvent<?> watchEvent) {
        log.debug("Invoked for [{}]", watchEvent.context().toString());
        handleTlsConfigChange();
    }

    private void handleTlsConfigChange() {
        log.info("Current reload count = {}", NUM_OF_CONFIG_CHANGES_SINCE_START.incrementAndGet());
        channels.flushStopAndRefresh();
    }
}
