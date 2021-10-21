/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.segment.impl;

import java.util.UUID;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.control.impl.Controller;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.shared.NameUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@VisibleForTesting
@RequiredArgsConstructor
public class SegmentOutputStreamFactoryImpl implements SegmentOutputStreamFactory {

    private final Controller controller;
    private final ConnectionPool cp;
    private final Consumer<Segment> nopSegmentSealedCallback = s -> log.error("Transaction segment: {} cannot be sealed", s);

    @Override
    public SegmentOutputStream createOutputStreamForTransaction(Segment segment, UUID txId, EventWriterConfig config,
                                                                DelegationTokenProvider tokenProvider) {
        return new SegmentOutputStreamImpl(NameUtils.getTransactionNameFromId(segment.getScopedName(), txId),
                                           config.isEnableConnectionPooling(), controller, cp, UUID.randomUUID(), nopSegmentSealedCallback,
                                           getRetryFromConfig(config), tokenProvider);
    }

    @Override
    public SegmentOutputStream createOutputStreamForSegment(Segment segment, Consumer<Segment> segmentSealedCallback,
                                                            EventWriterConfig config, DelegationTokenProvider tokenProvider) {
        SegmentOutputStreamImpl result =
                new SegmentOutputStreamImpl(segment.getScopedName(), config.isEnableConnectionPooling(), controller, cp, UUID.randomUUID(), segmentSealedCallback,
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
        return new SegmentOutputStreamImpl(segment.getScopedName(), config.isEnableConnectionPooling(), controller, cp, UUID.randomUUID(),
                                           Callbacks::doNothing, getRetryFromConfig(config), tokenProvider);
    }

    private RetryWithBackoff getRetryFromConfig(EventWriterConfig config) {
        return Retry.withExpBackoff(config.getInitialBackoffMillis(), config.getBackoffMultiple(),
                                    config.getRetryAttempts(), config.getMaxBackoffMillis());
    }
}
