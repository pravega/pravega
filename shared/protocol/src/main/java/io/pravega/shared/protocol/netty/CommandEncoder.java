/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.pravega.shared.protocol.netty.WireCommands.AppendBlock;
import io.pravega.shared.protocol.netty.WireCommands.AppendBlockEnd;
import io.pravega.shared.protocol.netty.WireCommands.Padding;
import io.pravega.shared.protocol.netty.WireCommands.PartialEvent;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.UUID;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.protocol.netty.WireCommands.TYPE_PLUS_LENGTH_SIZE;
import static io.pravega.shared.protocol.netty.WireCommands.TYPE_SIZE;

/**
 * Encodes data so that it can go out onto the wire.
 * For more details about the various commands @see WireCommands.
 *
 * The general encoding for commands is:
 * Type - 4 byte tag
 * Length - 4 byte length
 * Data - Which is obtained by calling the serializer for the specific wire command.
 *
 * Most commands are that simple. For performance Appends however are handled differently.
 *
 * If the channel is free, then the Appends are written in blocks so that the server does
 * not need to decode the contents of the block.
 * The block identifies which stream is appending so that each event does not have to and each each
 * event does not
 * have to be parsed individually. Events inside the block are encoded normally (with their Type and
 * Length). If an event does not fully fit inside of a block it can be wrapped in a PartialEvent
 * command. In this case the fist part of the Event is written as the value of the PartialEvent and
 * the remainder goes in the AppendBlockEnd.
 *
 * The AppendBlockEnd contains metadata about the block that was just appended so that it does not
 * need to be parsed out of individual messages. Notably this includes the event number of the last
 * event in the block, so that it can be acknowledged.
 *
 * if the channel is not free, then each append is enqueued to the session's pending List.
 * This session pending list gets flushed under following conditions
 *      if the session buffer exceed the threshold (MAX_DATA_SIZE)
 *      if the total number of events exceeds the threshold (MAX_EVENTS)
 *      if the block timeout is triggered.
 *
 */
@NotThreadSafe
@RequiredArgsConstructor
@Slf4j
public class CommandEncoder extends MessageToByteEncoder<Object> {
    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];
    private final Function<Long, AppendBatchSizeTracker> appendTracker;
    private final Map<Map.Entry<String, UUID>, Session> setupSegments = new HashMap<>();
    private final AtomicLong tokenCounter = new AtomicLong(0);
    private String segmentBeingAppendedTo;
    private UUID writerIdPerformingAppends;
    private int currentBlockSize;
    private int bytesLeftInBlock;
    private final Map<UUID, Session> pendingWrites = new HashMap<>();

    @RequiredArgsConstructor
    private final class Session {
        private static final int MAX_EVENTS = 500;
        private static final int MAX_DATA_SIZE = 1024 * 1024; // 1MB
        private final UUID id;
        private final long requestId;
        private final List<ByteBuf> pendingList = new ArrayList<>();
        private int  pendingBytes = 0;
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
         * queue the Append data to Session' list.
         * @param data  data bytes.
         * @param out   Network channel buffer.
         */
        private void write(ByteBuf data, ByteBuf out) {
            pendingWrites.putIfAbsent(id, this);
            if (data.readableBytes() > 0) {
                pendingBytes += data.readableBytes();
                pendingList.add(data);
            }
            conditionalFlush(out);
        }

        /**
         * check the overflow condition and flush if required.
         * @param out   Network channel buffer.
         */
        private void conditionalFlush(ByteBuf out) {
            if ((pendingBytes > MAX_DATA_SIZE) || (eventCount > MAX_EVENTS)) {
                breakCurrentAppend(out);
                flush(out);
            }
        }

       /**
        * Write/flush session's data to network channel.
        * @param out   Network channel buffer.
        */
       private void flush(ByteBuf out) {
            if (!isFree()) {
                pendingWrites.remove(id);
                if (pendingBytes > 0) {
                    writeMessage(new AppendBlock(id), pendingBytes, out);
                    for (Iterator<ByteBuf> iterator = pendingList.iterator(); iterator.hasNext(); ) {
                        out.writeBytes(iterator.next());
                        iterator.remove();
                    }
                }
                flush(pendingBytes, null, out);
                pendingBytes = 0;
            }
        }

        /**
         * Write/flush given data to network channel.
         * by writing the Append block End containing the given/input data and indicating the size of previous
         * append block data.
         * @param out   Network channel buffer.
         */
        private void flush(int sizOfWholeEvents, ByteBuf data, ByteBuf out) {
            writeMessage(new AppendBlockEnd(id, sizOfWholeEvents, data, eventCount, lastEventNumber, requestId), out);
            eventCount = 0;
        }
    }

    /**
     * Write/flush Multi session's buffered data to network channel buffer
     * by iterating over all the pending sessions .
     * @param out   Network Channel Buffer.
     */
    private void flushAll(ByteBuf out) {
        if (!pendingWrites.isEmpty()) {
            ArrayList<Session> sessions = new ArrayList<>(pendingWrites.values());
            sessions.forEach(session -> session.flush(out));
        }
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        log.trace("Encoding message to send over the wire {}", msg);
        if (msg instanceof Append) {
            Append append = (Append) msg;
            Session session = setupSegments.get(new SimpleImmutableEntry<>(append.segment, append.getWriterId()));
            validateAppend(append, session);
            final ByteBuf data = append.getData().slice();
            final int msgSize = data.readableBytes();
            final AppendBatchSizeTracker blockSizeSupplier = (appendTracker == null) ? null :
                    appendTracker.apply(append.getFlowId());

            if (blockSizeSupplier != null) {
                blockSizeSupplier.recordAppend(append.getEventNumber(), msgSize);
            }

            if (isChannelFree()) {
                if (session.isFree()) {
                    session.record(append);
                    startAppend(ctx, blockSizeSupplier, append, out);
                    continueAppend(data, out);
                    if (bytesLeftInBlock == 0) {
                        completeAppend(null, out);
                    }
                } else {
                    session.record(append);
                    session.write(data, out);
                    session.flush(out);
                }
            } else {
                session.record(append);
                if (isChannelOwner(append.getWriterId(), append.getSegment())) {
                    if (bytesLeftInBlock > data.readableBytes()) {
                        continueAppend(data, out);
                    } else {
                        ByteBuf dataInsideBlock = data.readSlice(bytesLeftInBlock);
                        completeAppend(dataInsideBlock, data, out);
                        flushAll(out);
                    }
                } else {
                      session.write(data, out);
                }
            }
        } else if (msg instanceof SetupAppend) {
            breakCurrentAppend(out);
            flushAll(out);
            writeMessage((SetupAppend) msg, out);
            SetupAppend setup = (SetupAppend) msg;
            setupSegments.put(new SimpleImmutableEntry<>(setup.getSegment(), setup.getWriterId()),
                              new Session(setup.getWriterId(), setup.getRequestId()));
        } else if (msg instanceof BlockTimeout) {
            BlockTimeout timeoutMsg = (BlockTimeout) msg;
            if (tokenCounter.get() == timeoutMsg.token) {
                breakCurrentAppend(out);
                flushAll(out);
            }
        } else if (msg instanceof WireCommand) {
            breakCurrentAppend(out);
            flushAll(out);
            writeMessage((WireCommand) msg, out);
        } else {
            throw new IllegalArgumentException("Expected a wire command and found: " + msg);
        }
    }

    private void validateAppend(Append append, Session session) {
        if (append.getEventCount() <= 0) {
            throw new InvalidMessageException("Invalid eventCount : " + append.getEventCount() +
                    " in the append for Writer id: " + append.getWriterId());
        }
        if (session == null || !session.id.equals(append.getWriterId())) {
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
     * @param writerID  Writers UUID.
     * @param segment   Segment.
     * @return true if channel is used by the given WriterId and Segment; false otherwise.
     */
    private boolean isChannelOwner(UUID writerID, String segment) {
        return writerID.equals(writerIdPerformingAppends) && segment.equals(segmentBeingAppendedTo);
    }

    /**
     * Start the Append.
     * if the Append batch size tracker decides the block size
     * which is more than event message size then timeout will be initiated.
     * Append block is written on channel.
     *
     * @param ctx               ChannelHandlerContext.
     * @param blockSizeSupplier Append batch size tracker.
     * @param append            Append data.
     * @param out               channel Buffer.
     */
    private void startAppend(ChannelHandlerContext ctx, AppendBatchSizeTracker blockSizeSupplier, Append append, ByteBuf out) {
        final int msgSize = append.getData().readableBytes();
        int blkSize = 0;
        if (blockSizeSupplier != null) {
            blkSize = blockSizeSupplier.getAppendBlockSize();
        }
        segmentBeingAppendedTo = append.segment;
        writerIdPerformingAppends = append.writerId;
        if (ctx != null && blkSize > msgSize) {
            currentBlockSize = blkSize;
            writeMessage(new AppendBlock(writerIdPerformingAppends), currentBlockSize + TYPE_PLUS_LENGTH_SIZE, out);
            ctx.executor().schedule(new BlockTimeouter(ctx.channel(), tokenCounter.incrementAndGet()),
                    blockSizeSupplier.getBatchTimeout(),
                    TimeUnit.MILLISECONDS);
        } else {
            currentBlockSize = msgSize;
            writeMessage(new AppendBlock(writerIdPerformingAppends), currentBlockSize, out);
        }
        bytesLeftInBlock = currentBlockSize;
    }

    /**
     * Continue the ongoing append by writing bytes to channel buffer.
     *
     * @param data  data to write.
     * @param out   channel Buffer.
     */
    private void continueAppend(ByteBuf data, ByteBuf out) {
        bytesLeftInBlock -= data.readableBytes();
        out.writeBytes(data);
    }

    /**
     * Complete the ongoing append by writing the pending data as Append Block End to channel buffer.
     *
     * @param pendingData   data to write.
     * @param out           channel Buffer.
     */
    private void completeAppend(ByteBuf pendingData, ByteBuf out) {
        Session session = setupSegments.get(new SimpleImmutableEntry<>(segmentBeingAppendedTo, writerIdPerformingAppends));
        session.flush(currentBlockSize - bytesLeftInBlock, pendingData, out);
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
     * @param out           channel Buffer.
     */
    private void completeAppend(ByteBuf data, ByteBuf pendingData, ByteBuf out) {
        writeMessage(new PartialEvent(data), out);
        completeAppend(pendingData, out);
    }


    /**
     * Break the ongoing append by padding zero bytes to remaining block size followed by Append block End.
     *
     * @param out   channel Buffer.
     */
    private void breakCurrentAppend(ByteBuf out) {
        if (isChannelFree()) {
            return;
        }
        writeMessage(new Padding(bytesLeftInBlock), out);
        completeAppend(null, out);
    }

    @SneakyThrows(IOException.class)
    private void writeMessage(AppendBlock block, int blkSize, ByteBuf out) {
        int startIdx = out.writerIndex();
        ByteBufOutputStream bout = new ByteBufOutputStream(out);
        bout.writeInt(block.getType().getCode());
        bout.write(LENGTH_PLACEHOLDER);
        block.writeFields(bout);
        bout.flush();
        bout.close();
        int endIdx = out.writerIndex();
        int fieldsSize = endIdx - startIdx - TYPE_PLUS_LENGTH_SIZE;
        out.setInt(startIdx + TYPE_SIZE, fieldsSize + blkSize);
    }

    @SneakyThrows(IOException.class)
    private int writeMessage(WireCommand msg, ByteBuf out) {
        int startIdx = out.writerIndex();
        ByteBufOutputStream bout = new ByteBufOutputStream(out);
        bout.writeInt(msg.getType().getCode());
        bout.write(LENGTH_PLACEHOLDER);
        msg.writeFields(bout);
        bout.flush();
        bout.close();
        int endIdx = out.writerIndex();
        int fieldsSize = endIdx - startIdx - TYPE_PLUS_LENGTH_SIZE;
        out.setInt(startIdx + TYPE_SIZE, fieldsSize);
        return endIdx - startIdx;
    }

    @RequiredArgsConstructor
    private static final class BlockTimeout {
        private final long token;
    }

    @RequiredArgsConstructor
    private final class BlockTimeouter implements Runnable {
        private final Channel channel;
        private final long token;

        @Override
        public void run() {
            if (tokenCounter.get() == token) {
                channel.writeAndFlush(new BlockTimeout(token));
            }
        }
    }
}
