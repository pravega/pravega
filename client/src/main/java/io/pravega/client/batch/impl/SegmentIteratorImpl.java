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
package io.pravega.client.batch.impl;

import com.google.common.annotations.Beta;
import io.pravega.client.ClientConfig;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TruncatedDataException;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;

import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.shared.security.auth.AccessOperation;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Beta
@Slf4j
public class SegmentIteratorImpl<T> implements SegmentIterator<T> {

    private final Segment segment;
    private final Serializer<T> deserializer;
    @Getter
    private final long startingOffset;
    private final long endingOffset;
    private final EventSegmentReader input;
    private final ClientConfig clientConfig;
    private final Controller controller;
    private final SegmentMetadataClientFactory segmentMetadataClientFactory;
    private final Retry.RetryWithBackoff backoffSchedule = Retry.withExpBackoff(1, 10, 9, 30000);

    public SegmentIteratorImpl(SegmentInputStreamFactory factory, SegmentMetadataClientFactory segmentMetadataClientFactory,
                               Controller controller, ClientConfig clientConfig, Segment segment,
                               Serializer<T> deserializer, long startingOffset, long endingOffset) {
        this.segment = segment;
        this.deserializer = deserializer;
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.segmentMetadataClientFactory = segmentMetadataClientFactory;
        this.controller = controller;
        this.clientConfig = clientConfig;
        input = factory.createEventReaderForSegment(segment, startingOffset, endingOffset);
    }

    @Override
    public boolean hasNext() {
        return input.getOffset() < endingOffset;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        // retry in-case of an empty ByteBuffer
        ByteBuffer read =
                backoffSchedule.retryWhen(t -> t instanceof TimeoutException)
                               .run(() -> {
                                   try {
                                       ByteBuffer buffer = input.read();
                                       if (buffer == null) {
                                           log.warn("Empty buffer while reading from Segment {} at offset {}",
                                                   input.getSegmentId(), input.getOffset());
                                           throw new TimeoutException(input.toString());
                                       }
                                       return buffer;
                                   } catch (NoSuchSegmentException | SegmentTruncatedException e) {
                                       handleSegmentTruncated(segment);
                                       throw new TruncatedDataException("Segment " + segment + " has been truncated.");
                                   }
                               });

        return deserializer.deserialize(read);
    }

    private void handleSegmentTruncated(Segment segmentId)  {
        log.info("{} encountered truncation for segment while read{} ", this, segmentId);
        @Cleanup
        SegmentMetadataClient metadataClient = segmentMetadataClientFactory.createSegmentMetadataClient(segmentId,
                DelegationTokenProviderFactory.create(controller, segmentId, AccessOperation.READ));
        try {
            input.setOffset(Futures.getThrowingExceptionWithTimeout(metadataClient.fetchCurrentSegmentHeadOffset(),
                    clientConfig.getConnectTimeoutMilliSec()));
        } catch (TimeoutException te) {
            log.warn("A timeout has occurred while attempting to retrieve segment information from the server for segment {}", segmentId);
        }
    }

    @Override
    public long getOffset() {
        return input.getOffset();
    }

    @Override
    public void close() {
        input.close();
    }

}
