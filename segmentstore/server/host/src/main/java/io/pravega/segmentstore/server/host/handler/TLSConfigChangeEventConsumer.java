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

import io.netty.channel.group.ChannelGroup;

import java.nio.file.WatchEvent;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.net.ssl.SSLException;

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
    @SuppressWarnings("checkstyle:ConstantName") // This is not intended to be used as a constant.
    public static final AtomicInteger countOfEventsConsumed = new AtomicInteger(0);

    /**
     * The path to the PEM-encoded server certificate file.
     */
    private @NonNull final String serverCertificatePath;

    /**
     * The path to the PEM-encoded file containing the server's private key.
     */
    private @NonNull final String serverKeyPath;

    @Override
    public void accept(WatchEvent<?> watchEvent) {
        log.debug("Invoked for [{}]", watchEvent.context().toString());
        handleTlsConfigChange();
    }

    private void handleTlsConfigChange() {
        log.info("Current reload count = {}", countOfEventsConsumed.incrementAndGet());

        ChannelGroup channels = Channels.get();
        channels.stream()
                // We don't care about channels that are not registered and channels that don't already have
                // a handler with the specified name.
                .filter(c -> c.isRegistered() && c.pipeline().get(TLSHelper.TLS_HANDLER_NAME) != null)
                .forEach(c -> {
                    try {
                        log.info("Pipeline before handling the change: [{}].", c.pipeline());
                        c.pipeline().replace(TLSHelper.TLS_HANDLER_NAME, TLSHelper.TLS_HANDLER_NAME,
                                TLSHelper.createServerSslContext(this.serverCertificatePath, this.serverKeyPath)
                                        .newHandler(c.alloc()));
                        log.info("Done replacing SSL Context handler. Pipeline after handling the change: [{}].",
                                c.pipeline());
                    } catch (SSLException e) {
                        log.warn(e.getMessage(), e);
                        c.close();
                    }
                });
    }
}
