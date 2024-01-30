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

package io.pravega.client.stream.impl;

import io.pravega.client.ClientConfig;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.SegmentReader;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.security.auth.AccessOperation;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

@Slf4j
public class SegmentReaderImpl<T> implements SegmentReader<T> {

    private final Segment segment;
    private final Serializer<T> deserializer;
    private final EventSegmentReader input;
    private final ClientConfig clientConfig;
    private final SegmentMetadataClient metadataClient;

    public SegmentReaderImpl(SegmentInputStreamFactory factory, Segment segment, Serializer<T> deserializer,
                             long startOffset, ClientConfig clientConfig, Controller controller,
                             SegmentMetadataClientFactory segmentMetadataClientFactory) {

        this.segment = segment;
        this.deserializer = deserializer;
        this.clientConfig = clientConfig;
        DelegationTokenProvider delegationTokenProvider = DelegationTokenProviderFactory.create(controller, segment, AccessOperation.READ);
        metadataClient = segmentMetadataClientFactory.createSegmentMetadataClient(segment,
                delegationTokenProvider);
        this.input = factory.createEventReaderForSegment(segment);
        this.input.setOffset(startOffset);
    }

    @Override
    public T read(long timeoutMillis) throws TruncatedDataException, EndOfSegmentException {
        try {
            ByteBuffer read = input.read();
            return read != null ? deserializer.deserialize(read) : null;
        } catch (SegmentTruncatedException e) {
            handleSegmentTruncated();
            throw new TruncatedDataException("Segment " + segment + " has been truncated.");
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return Futures.toVoid(input.fillBuffer());
    }

    private void handleSegmentTruncated()  {
        log.info("{} encountered truncation for segment while read{} ", this, segment);
        try {
            input.setOffset(Futures.getThrowingExceptionWithTimeout(metadataClient.fetchCurrentSegmentHeadOffset(),
                    clientConfig.getConnectTimeoutMilliSec()));
        } catch (TimeoutException te) {
            log.warn("A timeout has occurred while attempting to retrieve segment information from the server for segment {}", segment);
        }
    }

    @Override
    public void close() {
        metadataClient.close();
        input.close();
    }

}
