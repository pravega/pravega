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
package io.pravega.client.connection.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.pravega.common.Exceptions;
import io.pravega.shared.metrics.MetricNotifier;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.InvalidMessageException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands.AppendBlock;
import io.pravega.shared.protocol.netty.WireCommands.AppendBlockEnd;
import io.pravega.shared.protocol.netty.WireCommands.Hello;
import io.pravega.shared.protocol.netty.WireCommands.PartialEvent;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import java.io.IOException;
import java.io.OutputStream;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.io.StreamHelpers.closeQuietly;
import static io.pravega.shared.NameUtils.segmentTags;
import static io.pravega.shared.metrics.ClientMetricKeys.CLIENT_APPEND_BLOCK_SIZE;
import static io.pravega.shared.protocol.netty.WireCommands.TYPE_PLUS_LENGTH_SIZE;
import static io.pravega.shared.protocol.netty.WireCommands.TYPE_SIZE;

@Slf4j
@RequiredArgsConstructor
public class CommandEncoder {
    
    @VisibleForTesting
    static final int MAX_QUEUED_EVENTS = 500;
    @VisibleForTesting
    static final int MAX_QUEUED_SIZE = 1024 * 1024; // 1MB
    @VisibleForTesting
    static final int MAX_SETUP_SEGMENTS_SIZE = 2000;

    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];
    private final Function<Long, AppendBatchSizeTracker> appendTracker;
    private final MetricNotifier metricNotifier;
    @GuardedBy("$lock")
    private final Map<Map.Entry<String, UUID>, Session> setupSegments = new HashMap<>();
    private final AtomicLong tokenCounter = new AtomicLong(0);
    @GuardedBy("$lock")
    private String segmentBeingAppendedTo;
    @GuardedBy("$lock")
    private UUID writerIdPerformingAppends;
    @GuardedBy("$lock")
    private int currentBlockSize;
    @GuardedBy("$lock")
    private int bytesLeftInBlock;
    @GuardedBy("$lock")
    private final Map<UUID, Session> pendingWrites = new HashMap<>();

    private final OutputStream output;
    private final ByteBuf buffer = Unpooled.buffer(1024 * 1024);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ReplyProcessor callback;
    private final PravegaNodeUri location;

    @RequiredArgsConstructor
    @VisibleForTesting
    final class Session {
        @Getter
        private final UUID id;
        private final long requestId;
        private final List<ByteBuf> pendingList = new ArrayList<>();
        private int pendingBytes = 0;
        private long lastEventNumber = -1L;
        private int eventCount = 0;

        /**
         * Record the given Append to Session by tracking the last event number and number of events to write.
         */
        private void record(Append append) {
            lastEventNumber = append.getEventNumber();
            eventCount += append.getEventCount();
        }

        /**
         * Check if session is empty or not.
         * @return true if there are any pending events; false otherwise.
         */
        private boolean isFree() {
            return eventCount == 0;
        }

        /**
         * Queue the Append data to Session' list.
         *
         * @param data  data bytes.
         * @throws IOException If the write to the outputstream fails
         */
        private void write(ByteBuf data) throws IOException {
            pendingWrites.putIfAbsent(id, this);
            if (data.readableBytes() > 0) {
                pendingBytes += data.readableBytes();
                pendingList.add(data);
            }
            conditionalFlush();
        }

        /**
         * Check the overflow condition and flush if required.
         *
         * @throws IOException If the write to the outputstream fails
         */
        private void conditionalFlush() throws IOException {
            if ((pendingBytes > MAX_QUEUED_SIZE) || (eventCount > MAX_QUEUED_EVENTS)) {
                breakCurrentAppend();
                flushAllToBuffer();
                flushBuffer();
            }
        }

       /**
        * Write/flush session's data to network channel.
        */
       private void flushToBuffer() {
            if (!isFree()) {
                pendingWrites.remove(id);
                writeMessage(new AppendBlock(id), pendingBytes);
                if (pendingBytes > 0) {
                    pendingList.forEach(buffer::writeBytes);
                    pendingList.clear();
                }
                flushToBuffer(pendingBytes, null);
                pendingBytes = 0;
            }
        }

        /**
         * Write/flush given data to network channel by writing the Append block End containing the given/input data
         * and indicating the size of previous append block data.
         *
         * @param sizeOfWholeEvents Size of data followed by this append block end.
         * @param data              Remaining data
         */
        private void flushToBuffer(int sizeOfWholeEvents, ByteBuf data) {
            writeMessage(new AppendBlockEnd(id, sizeOfWholeEvents, data, eventCount, lastEventNumber, requestId), buffer);
            eventCount = 0;
        }
    }
    
    /**
     * Write/flush multi session's buffered data to network channel buffer by iterating over all the pending sessions .
     */
    private void flushAllToBuffer() {
        if (!pendingWrites.isEmpty()) {
            ArrayList<Session> sessions = new ArrayList<>(pendingWrites.values());
            sessions.forEach(Session::flushToBuffer);
        }
    }
    
    @Synchronized
    public void write(WireCommand msg) throws IOException {
        Exceptions.checkNotClosed(this.closed.get(), this);

        if (msg instanceof SetupAppend) {
            breakCurrentAppend();
            flushAllToBuffer();
            if (setupSegments.size() >= MAX_SETUP_SEGMENTS_SIZE) {
                log.debug("CommandEncoder {} setupSegments map reached maximum size of {}", this.location, MAX_SETUP_SEGMENTS_SIZE);
                flushBuffer();
                closed.compareAndSet(false, true);
                if (callback != null) {
                    callback.connectionDropped();
                }
                throw new IOException("CommandEncoder " + this.location + " closed due to memory limit reached");
            }
            writeMessage(msg, buffer);
            SetupAppend setup = (SetupAppend) msg;
            setupSegments.put(new SimpleImmutableEntry<>(setup.getSegment(), setup.getWriterId()),
                              new Session(setup.getWriterId(), setup.getRequestId()));
            flushBuffer();
        } else if (msg instanceof Hello) {
            Preconditions.checkState(isChannelFree());
            Preconditions.checkState(pendingWrites.isEmpty());
            writeMessage(msg, buffer);
            flushBuffer();
        } else {
            breakCurrentAppend();
            flushAllToBuffer();
            writeMessage(msg, buffer);
            flushBuffer();
        }
    }
    
    @Synchronized
    public void write(Append append) throws IOException {
        Exceptions.checkNotClosed(this.closed.get(), this);

        Session session = setupSegments.get(new SimpleImmutableEntry<>(append.getSegment(), append.getWriterId()));
        validateAppend(append, session);
        final ByteBuf data = append.getData().slice();
        final AppendBatchSizeTracker blockSizeSupplier = (appendTracker == null) ? null :
                appendTracker.apply(append.getFlowId());

        if (blockSizeSupplier != null) {
            blockSizeSupplier.recordAppend(append.getEventNumber(), data.readableBytes());
        }
        if (isChannelFree()) {
            if (session.isFree()) {
                session.record(append);
                startAppend(blockSizeSupplier, append);
                continueAppend(data);
                if (bytesLeftInBlock == 0) {
                    completeAppend(null);
                    flushBuffer();
                }
            } else {
                session.record(append);
                session.write(data);
                session.flushToBuffer();
            }
        } else {
            session.record(append);
            if (isChannelOwner(append.getWriterId(), append.getSegment())) {
                if (bytesLeftInBlock > data.readableBytes()) {
                    continueAppend(data);
                } else {
                    ByteBuf dataInsideBlock = data.readSlice(bytesLeftInBlock);
                    completeAppend(dataInsideBlock, data);
                    flushAllToBuffer();
                    flushBuffer();
                }
            } else {
                  session.write(data);
            }
        }
    }
    
    @GuardedBy("$lock")
    private void flushBuffer() throws IOException {
        buffer.getBytes(buffer.readerIndex(), output, buffer.readableBytes());
        buffer.clear();
    }
    
    @VisibleForTesting
    static void validateAppend(Append append, Session session) {
        if (append.getEventCount() <= 0) {
            throw new InvalidMessageException("Invalid eventCount : " + append.getEventCount() +
                    " in the append for Writer id: " + append.getWriterId());
        }
        if (session == null || !session.getId().equals(append.getWriterId())) {
            throw new InvalidMessageException("Sending appends without setting up the append. Append Writer id: " + append.getWriterId());
        }
        if (append.getEventNumber() <= session.lastEventNumber) {
            throw new InvalidMessageException("Events written out of order. Received: " + append.getEventNumber() +
            " following: " + session.lastEventNumber +
            " for Writer id: " + append.getWriterId());
        }
        if (append.isConditional()) {
            throw new IllegalArgumentException("Conditional appends should be written via a ConditionalAppend object.");
        }
    }

    /**
     * Check if the network channel is idle or not.
     *
     * @return true if channel is not used by any writers; false otherwise.
     */
    private boolean isChannelFree() {
        return writerIdPerformingAppends == null;
    }

    /**
     * Check/Confirm the Network channel Ownership.
     * The Network channel ownership is associated with Writer ID and segment to which writes are performed.
     *
     * @param writerID  Writer's UUID.
     * @param segment   Segment.
     * @return true if channel is used by the given WriterId and Segment; false otherwise.
     */
    private boolean isChannelOwner(UUID writerID, String segment) {
        return writerID.equals(writerIdPerformingAppends) && segment.equals(segmentBeingAppendedTo);
    }

    /**
     * Start the Append.
     * Append block is written on channel
     * If the Append batch size tracker decides the block size which is more than event message size then
     * the block timeout will be initiated with a token.
     *
     * @param blockSizeSupplier Append batch size tracker.
     * @param append            Append data.
     */
    private void startAppend(AppendBatchSizeTracker blockSizeSupplier, Append append) {
        final int msgSize = append.getData().readableBytes();
        int blockSize = blockSizeSupplier.getAppendBlockSize();
        // Only publish client side metrics when there is some metrics notifier configured for efficiency.
        if (metricNotifier != null && !metricNotifier.equals(MetricNotifier.NO_OP_METRIC_NOTIFIER)) {
            metricNotifier.updateSuccessMetric(CLIENT_APPEND_BLOCK_SIZE, segmentTags(append.getSegment(), append.getWriterId().toString()),
                                               blockSize);
        }

        segmentBeingAppendedTo = append.getSegment();
        writerIdPerformingAppends = append.getWriterId();
        if (blockSize > msgSize) {
            currentBlockSize = blockSize;
            writeMessage(new AppendBlock(writerIdPerformingAppends), currentBlockSize + TYPE_PLUS_LENGTH_SIZE);
            tokenCounter.incrementAndGet();
        } else {
            currentBlockSize = msgSize;
            writeMessage(new AppendBlock(writerIdPerformingAppends), currentBlockSize);
        }
        bytesLeftInBlock = currentBlockSize;
    }

    /**
     * Continue the ongoing append by writing bytes to channel buffer.
     *
     * @param data  data to write.
     */
    private void continueAppend(ByteBuf data) {
        bytesLeftInBlock -= data.readableBytes();
        buffer.writeBytes(data);
    }

    /**
     * Complete the ongoing append by writing the pending data as Append Block End to channel buffer.
     * Increment the Token Counter value so the block timeout need not invoke the flush again.
     *
     * @param pendingData   data to write.
     */
    private void completeAppend(ByteBuf pendingData) {
        Session session = setupSegments.get(new SimpleImmutableEntry<>(segmentBeingAppendedTo, writerIdPerformingAppends));
        session.flushToBuffer(currentBlockSize - bytesLeftInBlock, pendingData);
        tokenCounter.incrementAndGet();
        bytesLeftInBlock = 0;
        currentBlockSize = 0;
        segmentBeingAppendedTo = null;
        writerIdPerformingAppends = null;
    }

    /**
     * Complete the ongoing append by writing the partial Event to ongoing append to channel buffer.
     * and the remaining data as Append Block End to channel buffer.
     *
     * @param data          Partial Event.
     * @param pendingData   data to write.
     */
    private void completeAppend(ByteBuf data, ByteBuf pendingData) {
        writeMessage(new PartialEvent(data), buffer);
        completeAppend(pendingData);
    }

    /**
     * Break the ongoing append by padding zero bytes to remaining block size followed by Append block End.
     */
    private void breakCurrentAppend() {
        if (isChannelFree()) {
            return;
        }
        writePadding();
        completeAppend(null);
    }
    
    private void writePadding() {
        buffer.writeInt(WireCommandType.PADDING.getCode());
        buffer.writeInt(bytesLeftInBlock);
        buffer.writeZero(bytesLeftInBlock);
    }

    @SneakyThrows(IOException.class)
    private void writeMessage(AppendBlock block, int blockSize) {
        int startIdx = buffer.writerIndex();
        ByteBufOutputStream bout = new ByteBufOutputStream(buffer);
        bout.writeInt(block.getType().getCode());
        bout.write(LENGTH_PLACEHOLDER);
        block.writeFields(bout);
        bout.flush();
        bout.close();
        int endIdx = buffer.writerIndex();
        int fieldsSize = endIdx - startIdx - TYPE_PLUS_LENGTH_SIZE;
        buffer.setInt(startIdx + TYPE_SIZE, fieldsSize + blockSize);
    }

    @SneakyThrows(IOException.class)
    @VisibleForTesting
    static int writeMessage(WireCommand msg, ByteBuf destination) {
        int startIdx = destination.writerIndex();
        ByteBufOutputStream bout = new ByteBufOutputStream(destination);
        bout.writeInt(msg.getType().getCode());
        bout.write(LENGTH_PLACEHOLDER);
        msg.writeFields(bout);
        bout.flush();
        bout.close();
        int endIdx = destination.writerIndex();
        int fieldsSize = endIdx - startIdx - TYPE_PLUS_LENGTH_SIZE;
        destination.setInt(startIdx + TYPE_SIZE, fieldsSize);
        return endIdx - startIdx;
    }
    
    
    /**
     * This method is called periodically to close any open batches which exceed their timeouts.
     * @return a token which is used to identify which batch is open.
     * @param token the token returned by the previous call to this method or -1 if this is the first call to this method.
     */
    @Synchronized
    public long batchTimeout(long token) {
        long result = tokenCounter.get();
        try {
            if (result == token) {
                breakCurrentAppend();
                flushAllToBuffer();
                flushBuffer();
            }
        } catch (IOException e) {
            log.error("Failed to time out block. Closing connection.", e);
            closeQuietly(output, log, "Closing output failed");
        }
        return result;
    }
}
