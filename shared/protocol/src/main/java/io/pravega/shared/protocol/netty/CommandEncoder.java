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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.netty.buffer.Unpooled.buffer;
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
 * Appends are written in blocks so that the server does not need to decode the contents of the
 * block.
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
 */
@NotThreadSafe
@RequiredArgsConstructor
@Slf4j
public class CommandEncoder extends MessageToByteEncoder<Object> {
    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];
    private static final int MAX_DATA_SIZE = 4 * 1024 * 1024; // 4MB
    private final Function<Long, AppendBatchSizeTracker> appendTracker;
    private final Map<Map.Entry<String, UUID>, Session> setupSegments = new HashMap<>();
    private final AtomicLong tokenCounter = new AtomicLong(0);
    private String segmentBeingAppendedTo;
    private UUID writerIdPerformingAppends;
    private int currentBlockSize;
    private int bytesLeftInBlock;
    private final List<Session> pendingWrites = new ArrayList<>();
    private long totalBytes = 0;

    @RequiredArgsConstructor

    private final class Session {
        private static final int MAX_BLOCK_SIZE = 1024 * 1024;  // 1MB
        private static final int MAX_EVENTS = 500;
        private final UUID id;
        private final long requestId;
        private final ByteBuf data = buffer();
        private long lastEventNumber = -1L;
        private int eventCount = 0;

        private void recordAppend(Append append) {
            lastEventNumber = append.getEventNumber();
            eventCount += append.getEventCount();
        }

        private void append(ByteBuf buffer, ByteBuf out) {
            if (buffer != null && buffer.readableBytes() > 0) {
                totalBytes += buffer.readableBytes();
                if (data.readableBytes() == 0) {
                    pendingWrites.add(this);
                }
                data.writeBytes(buffer);
                if (totalBytes > MAX_DATA_SIZE) {
                    breakCurrentAppend(out);
                    flushAll(out);
                } else if ((data.readableBytes() > MAX_BLOCK_SIZE) ||
                        ((data.readableBytes() > 0) && (eventCount > MAX_EVENTS))) {
                    breakCurrentAppend(out);
                    flush(out);
                }
            }
        }

        private void flush(ByteBuf out) {
            if (data.readableBytes() > 0) {
                totalBytes -= data.readableBytes();
                writeMessage(new AppendBlockEnd(id, 0, data, eventCount, lastEventNumber, requestId), out);
                data.clear();
                eventCount = 0;
            }
        }
    }

    private void flushAll(ByteBuf out) {
        if (totalBytes > 0) {
            pendingWrites.forEach(session -> session.flush(out));
            pendingWrites.clear();
            totalBytes = 0;
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
            final AppendBatchSizeTracker blockSizeSupplier = (appendTracker == null) ? null :
                    appendTracker.apply(append.getFlowId());

            if (blockSizeSupplier != null) {
                blockSizeSupplier.recordAppend(append.getEventNumber(), data.readableBytes());
            }

            if (isChannelFree()) {
                session.flush(out);
                session.recordAppend(append);
                if (startAppend(ctx, blockSizeSupplier, append, out)) {
                    continueAppend(data, out);
               } else {
                    breakFromAppend(null, data, false, out);
               }
            } else {
                session.recordAppend(append);
                if (isChannelOwner(append.getWriterId(), append.getSegment())) {
                    if (bytesLeftInBlock > data.readableBytes()) {
                        continueAppend(data, out);
                    } else {
                        ByteBuf dataInsideBlock = data.readSlice(bytesLeftInBlock);
                        breakFromAppend(dataInsideBlock, data, true, out);
                        flushAll(out);
                    }
                } else {
                      session.append(data, out);
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
            throw new InvalidMessageException("Invalid eventCount in the append.");
        }
        if (session == null || !session.id.equals(append.getWriterId())) {
            throw new InvalidMessageException("Sending appends without setting up the append.");
        }
        if (append.getEventNumber() <= session.lastEventNumber) {
            throw new InvalidMessageException("Events written out of order. Received: " + append.getEventNumber()
            + " following: " + session.lastEventNumber);
        }
        if (append.isConditional()) {
            throw new IllegalArgumentException("Conditional appends should be written via a ConditionalAppend object.");
        }
    }

    private boolean isChannelFree() {
        return writerIdPerformingAppends == null;
    }

    private boolean isChannelOwner(UUID writerID, String segment) {
        return writerID.equals(writerIdPerformingAppends) && segment.equals(segmentBeingAppendedTo);
    }

    private boolean startAppend(ChannelHandlerContext ctx, AppendBatchSizeTracker blockSizeSupplier, Append append, ByteBuf out) {
        final int msgSize = append.getData().readableBytes();
        currentBlockSize = msgSize;
        if (blockSizeSupplier != null) {
            currentBlockSize = Math.max(currentBlockSize, blockSizeSupplier.getAppendBlockSize());
        }
        bytesLeftInBlock = currentBlockSize;
        segmentBeingAppendedTo = append.segment;
        writerIdPerformingAppends = append.writerId;
        if (ctx != null && currentBlockSize > msgSize) {
            writeMessage(new AppendBlock(writerIdPerformingAppends), currentBlockSize + TYPE_PLUS_LENGTH_SIZE, out);
            ctx.executor().schedule(new BlockTimeouter(ctx.channel(), tokenCounter.incrementAndGet()),
                    blockSizeSupplier.getBatchTimeout(),
                    TimeUnit.MILLISECONDS);
            return true;
        }
        return false;
    }

    private void continueAppend(ByteBuf data, ByteBuf out) {
        bytesLeftInBlock -= data.readableBytes();
        out.writeBytes(data);
    }

    private void breakFromAppend(ByteBuf data, ByteBuf pendingData,  boolean isAppend, ByteBuf out) {
        if (isChannelFree()) {
            return;
        }
        if (isAppend) {
            if (data != null) {
                writeMessage(new PartialEvent(data), out);
            } else {
                writeMessage(new Padding(bytesLeftInBlock), out);
            }
         }
        Session session = setupSegments.get(new SimpleImmutableEntry<>(segmentBeingAppendedTo, writerIdPerformingAppends));
        writeMessage(new AppendBlockEnd(session.id,
                currentBlockSize - bytesLeftInBlock,
                    pendingData,
                    session.eventCount,
                    session.lastEventNumber, session.requestId), out);
        bytesLeftInBlock = 0;
        currentBlockSize = 0;
        session.eventCount = 0;
        tokenCounter.incrementAndGet();
        segmentBeingAppendedTo = null;
        writerIdPerformingAppends = null;
    }

    private void breakCurrentAppend(ByteBuf out) {
        breakFromAppend(null, null, true, out);
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
