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
import io.pravega.client.segment.impl.SegmentInputStream;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.stream.EventReadWithStatus;
import io.pravega.client.stream.SegmentReader;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.InvalidMessageException;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.security.auth.AccessOperation;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class SegmentReaderImpl<T> implements SegmentReader<T> {

    /*
     * This timeout is the maximum amount of time the reader will wait in the case of partial event data being received
     *  by the client. After this timeout the client will resend the request.
     */
    static final long PARTIAL_DATA_TIMEOUT = TimeUnit.SECONDS.toMillis(30);

    // Base waiting time for a reader on an idle segment waiting for new data to be read.
    private static final long BASE_READER_WAITING_TIME_MS = 1000;
    private final Segment segment;
    private final Serializer<T> deserializer;
    private final SegmentInputStream input;
    private final ClientConfig clientConfig;
    private final SegmentMetadataClient metadataClient;
    private final ByteBuffer headerReadingBuffer = ByteBuffer.allocate(WireCommands.TYPE_PLUS_LENGTH_SIZE);

    public SegmentReaderImpl(SegmentInputStreamFactory factory, Segment segment, Serializer<T> deserializer,
                             long startOffset, ClientConfig clientConfig, Controller controller,
                             SegmentMetadataClientFactory segmentMetadataClientFactory) {

        this.segment = segment;
        this.deserializer = deserializer;
        this.clientConfig = clientConfig;
        DelegationTokenProvider delegationTokenProvider = DelegationTokenProviderFactory.create(controller, segment, AccessOperation.READ);
        metadataClient = segmentMetadataClientFactory.createSegmentMetadataClient(segment,
                delegationTokenProvider);
        this.input = factory.createInputStreamForSegment(segment, delegationTokenProvider, startOffset);
    }

    @Override
    public EventReadWithStatus<T> read(long timeoutMillis) throws TruncatedDataException {
        long firstByteTimeoutMillis = Math.min(timeoutMillis, BASE_READER_WAITING_TIME_MS);
        long originalOffset = input.getOffset();
        long traceId = LoggerHelpers.traceEnter(log, "read", input.getSegmentId(), originalOffset, firstByteTimeoutMillis);
        boolean success = false;
        boolean resendRequest = false;
        ByteBuffer result = null;
        Status status = Status.AVAILABLE_NOW;
        //If bytesInBuffer is more than header buffer size, then issue a read request and get the event information. Else
        // return empty buffer having status available later.
        try {
            headerReadingBuffer.clear();
            int bytesInBuffer = input.bytesInBuffer();
            //If outstanding request complete exceptionally then bytesInBuffer will return -1 and in read request will get the exception.
            boolean isAvailable = bytesInBuffer == -1 || bytesInBuffer > headerReadingBuffer.capacity();
            if (isAvailable  && (result = readEvent(firstByteTimeoutMillis)) != null) {
                success = true;
            } else {
               status = Status.AVAILABLE_LATER;
            }
        } catch (TimeoutException e) {
            resendRequest = true;
            status = Status.AVAILABLE_LATER;
            log.warn("Timeout observed while trying to read data from Segment store, the read request will be retransmitted");
        } catch (EndOfSegmentException e) {
            log.debug("Reached to end of segment for {}, end offset is {}.", segment, input.getOffset());
            status = getStatus(e);
        } catch (SegmentTruncatedException e) {
            log.debug("Reached to end of segment for {}, end offset is {}.", segment, input.getOffset());
            handleSegmentTruncated();
            throw new TruncatedDataException();
        } finally {
            LoggerHelpers.traceLeave(log, "read", traceId, input.getSegmentId(), originalOffset, firstByteTimeoutMillis, success);
            if (!success) {
                // Reading failed, reset the offset to the original offset.
                // The read request is retransmitted only in the case of a timeout.
                input.setOffset(originalOffset, resendRequest);
            }
            input.fillBuffer();
        }
        return new EventReadWithStatusImpl<>(result == null ? null : deserializer.deserialize(result), status);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        return input.fillBuffer().exceptionally(ex -> {
            if (Exceptions.unwrap(ex) instanceof SegmentTruncatedException) {
                result.completeExceptionally(new TruncatedDataException());
            } else if (Exceptions.unwrap(ex) instanceof EndOfSegmentException) {
                result.completeExceptionally(new EndOfSegmentException());
            } else {
                result.completeExceptionally(ex);
            }
            return null;
        }).thenCompose(x -> {
            //TODO : This check can be removed as fillBuffer itself ensure that once it will complete, there will be some data to read
            int bytesInBuffer = input.bytesInBuffer();
            if (bytesInBuffer != 0) {
                result.complete(null);
            }
            return result;
        });
    }

    private ByteBuffer readEvent(long firstByteTimeoutMillis) throws EndOfSegmentException, SegmentTruncatedException, TimeoutException {
        int read = input.read(headerReadingBuffer, firstByteTimeoutMillis);
        if (headerReadingBuffer.hasRemaining()) {
            log.debug("Unable to read event header for segment id {} at offset {}", input.getSegmentId(), input.getOffset());
            log.debug("Desired event size in bytes : {}. Total bytes read : {}.", headerReadingBuffer.capacity(), read);
            return null;
        }
        headerReadingBuffer.flip();
        int type = headerReadingBuffer.getInt();
        int length = headerReadingBuffer.getInt();
        if (type != WireCommandType.EVENT.getCode()) {
            throw new InvalidMessageException("Event was of wrong type: " + type);
        }
        if (length < 0) {
            throw new InvalidMessageException("Event of invalid length: " + length);
        }
        //TODO: Here we are making the direct read call at segmentInputStream without comparing the event length with bytesInBuff.
        // As SegmentInputStream is having buffer size as 1024 * 1024. It might be the case event size is more than buffer size.
        ByteBuffer result = ByteBuffer.allocate(length);

        readEventDataFromSegmentInputStream(result);
        while (result.hasRemaining()) {
            readEventDataFromSegmentInputStream(result);
        }
        result.flip();
        return result;
    }

    private void readEventDataFromSegmentInputStream(ByteBuffer result) throws EndOfSegmentException, SegmentTruncatedException, TimeoutException {
        //SSS can return empty events incase of @link{io.pravega.segmentstore.server.host.handler.PravegaRequestProcessor.ReadCancellationException}
        if (input.read(result, PARTIAL_DATA_TIMEOUT) == 0 && result.limit() != 0) {
            log.warn("Timeout while trying to read Event data from segment {} at offset {}. The buffer capacity is {} bytes and the data read so far is {} bytes",
                    input.getSegmentId(), input.getOffset(), result.limit(), result.position());
            throw new TimeoutException("Timeout while trying to read event data");
        }
    }

    private Status getStatus(EndOfSegmentException e) {
        Status status;
        if (e.getErrorType().equals(EndOfSegmentException.ErrorType.END_OF_SEGMENT_REACHED)) {
            status = Status.FINISHED;
        } else {
            status = Status.AVAILABLE_LATER;
        }
        return status;
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
