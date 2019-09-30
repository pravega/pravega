/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@VisibleForTesting
@RequiredArgsConstructor
public class SegmentOutputStreamFactoryImpl implements SegmentOutputStreamFactory {

    private final Controller controller;
    private final ConnectionFactory cf;
    private final Consumer<Segment> nopSegmentSealedCallback = s -> log.error("Transaction segment: {} cannot be sealed", s);

    @Override
    public SegmentOutputStream createOutputStreamForTransaction(Segment segment, UUID txId, EventWriterConfig config,
                                                                DelegationTokenProvider tokenProvider) {
        return new SegmentOutputStreamImpl(StreamSegmentNameUtils.getTransactionNameFromId(segment.getScopedName(), txId),
                                           config.isEnableConnectionPooling(), controller, cf, UUID.randomUUID(), nopSegmentSealedCallback,
                                           getRetryFromConfig(config), tokenProvider);
    }

    @Override
    public SegmentOutputStream createOutputStreamForSegment(Segment segment, Consumer<Segment> segmentSealedCallback,
                                                            EventWriterConfig config, DelegationTokenProvider tokenProvider) {
        SegmentOutputStreamImpl result =
                new SegmentOutputStreamImpl(segment.getScopedName(), config.isEnableConnectionPooling(), controller, cf, UUID.randomUUID(), segmentSealedCallback,
                                            getRetryFromConfig(config), tokenProvider);
        try {
            result.getConnection();
        } catch (RetriesExhaustedException | SegmentSealedException | NoSuchSegmentException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return result;
    }

    @Override
    public SegmentOutputStream createOutputStreamForSegment(Segment segment, EventWriterConfig config,
                                                            DelegationTokenProvider tokenProvider) {
        return new SegmentOutputStreamImpl(segment.getScopedName(), config.isEnableConnectionPooling(), controller, cf, UUID.randomUUID(),
                                           Callbacks::doNothing, getRetryFromConfig(config), tokenProvider);
    }

    private RetryWithBackoff getRetryFromConfig(EventWriterConfig config) {
        return Retry.withExpBackoff(config.getInitalBackoffMillis(), config.getBackoffMultiple(),
                                    config.getRetryAttempts(), config.getMaxBackoffMillis());
    }
}
