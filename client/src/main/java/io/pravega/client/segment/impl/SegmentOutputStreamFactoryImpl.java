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
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.shared.segment.StreamSegmentNameUtils;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
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
    private final Map<String, Map.Entry<SegmentOutputStreamImpl, SegmentSealedCallbacks>> setupSegments = new HashMap<>();

    private  static class SegmentSealedCallbacks {
        private final List<Consumer<Segment>> callbackList = new ArrayList<>();

        public void add(Consumer<Segment> segmentSealedCallback) {
            callbackList.add(segmentSealedCallback);
        }

        public void callBack(Segment segment) {
            callbackList.forEach(s -> s.accept(segment));
        }
    }

    @Override
    public SegmentOutputStream createOutputStreamForTransaction(Segment segment, UUID txId, EventWriterConfig config,
                                                                String delegationToken) {
        return new SegmentOutputStreamImpl(StreamSegmentNameUtils.getTransactionNameFromId(segment.getScopedName(), txId),
                                           config.isEnableConnectionPooling(), controller, cf, UUID.randomUUID(), nopSegmentSealedCallback,
                                           getRetryFromConfig(config), delegationToken);
    }

    @Override
    public SegmentOutputStream createOutputStreamForSegment(Segment segment, Consumer<Segment> segmentSealedCallback, EventWriterConfig config, String delegationToken) {
        SegmentOutputStreamImpl result;
        SegmentSealedCallbacks segmentSeal;

        String key = segment.toString();
        if (config.isEnableConnectionPooling()) {
            final Map.Entry<SegmentOutputStreamImpl, SegmentSealedCallbacks> entry = setupSegments.get(key);
            if (entry == null) {
                segmentSeal = new SegmentSealedCallbacks();
                result = new SegmentOutputStreamImpl(key, config.isEnableConnectionPooling(),
                        controller, cf, UUID.randomUUID(), segmentSeal::callBack, getRetryFromConfig(config), delegationToken);
                try {
                    result.getConnection();
                } catch (RetriesExhaustedException | SegmentSealedException | NoSuchSegmentException e) {
                    log.warn("Initial connection attempt failure. Suppressing.", e);
                }
                setupSegments.put(key, new SimpleImmutableEntry<>(result, segmentSeal));
            } else {
                segmentSeal = setupSegments.get(key).getValue();
                result = entry.getKey();
            }
            segmentSeal.add(segmentSealedCallback);
        } else {
            result = new SegmentOutputStreamImpl(key, config.isEnableConnectionPooling(), controller, cf,
                    UUID.randomUUID(), segmentSealedCallback, getRetryFromConfig(config), delegationToken);
            try {
                result.getConnection();
            } catch (RetriesExhaustedException | SegmentSealedException | NoSuchSegmentException e) {
                log.warn("Initial connection attempt failure. Suppressing.", e);
            }
        }
        return result;
    }
    
    @Override
    public SegmentOutputStream createOutputStreamForSegment(Segment segment, EventWriterConfig config, String delegationToken) {
        return createOutputStreamForSegment(segment, Callbacks::doNothing, config, delegationToken);
    }
    
    private RetryWithBackoff getRetryFromConfig(EventWriterConfig config) {
        return Retry.withExpBackoff(config.getInitalBackoffMillis(), config.getBackoffMultiple(),
                                    config.getRetryAttempts(), config.getMaxBackoffMillis());
    }
}
