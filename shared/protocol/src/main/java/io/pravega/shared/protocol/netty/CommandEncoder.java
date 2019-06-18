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

import com.google.common.base.Preconditions;
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
    private final Function<Long, AppendBatchSizeTracker> appendTracker;
    private final Map<Map.Entry<String, UUID>, Session> setupSegments = new HashMap<>();
    private AtomicLong tokenCounter = new AtomicLong(0);
    private String segmentBeingAppendedTo;
    private UUID writerIdPerformingAppends;
    private int currentBlockSize;
    private int bytesLeftInBlock;

    @RequiredArgsConstructor
    private static final class Session {
        private final UUID id;
        private long lastEventNumber = -1L;
        private int eventCount;
        private final long requestId;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        log.trace("Encoding message to send over the wire {}", msg);
        if (msg instanceof Append) {
            Append append = (Append) msg;
            if (!append.segment.equals(segmentBeingAppendedTo) || !append.getWriterId().equals(writerIdPerformingAppends)) {
                breakFromAppend(null, null, out);
            }
            ByteBuf data = append.getData().slice();
            int msgSize = data.readableBytes();
            AppendBatchSizeTracker blockSizeSupplier = null;
            Session session = setupSegments.get(new SimpleImmutableEntry<>(append.segment, append.getWriterId()));
            validateAppend(append, session);
            session.lastEventNumber = append.getEventNumber();
            session.eventCount++;
            if (appendTracker != null) {
                blockSizeSupplier = appendTracker.apply(append.getFlowId());
                blockSizeSupplier.recordAppend(append.getEventNumber(), msgSize);
            }
            if (bytesLeftInBlock == 0) {
                if (blockSizeSupplier != null) {
                    currentBlockSize = Math.max(TYPE_PLUS_LENGTH_SIZE, blockSizeSupplier.getAppendBlockSize());
                } else {
                    currentBlockSize = msgSize + TYPE_PLUS_LENGTH_SIZE;
                }
                bytesLeftInBlock = currentBlockSize;
                segmentBeingAppendedTo = append.segment;
                writerIdPerformingAppends = append.writerId;
                writeMessage(new AppendBlock(session.id), out);
                if (ctx != null && blockSizeSupplier != null && currentBlockSize > (msgSize + TYPE_PLUS_LENGTH_SIZE)) {
                    ctx.executor().schedule(new BlockTimeouter(ctx.channel(), tokenCounter.incrementAndGet()),
                                            blockSizeSupplier.getBatchTimeout(),
                                            TimeUnit.MILLISECONDS);
                }
            }
            // Is there enough space for a subsequent message after this one?
            if (bytesLeftInBlock - msgSize > TYPE_PLUS_LENGTH_SIZE) {
                out.writeBytes(data);
                bytesLeftInBlock -= msgSize;
            } else {
                ByteBuf dataInsideBlock = data.readSlice(bytesLeftInBlock - TYPE_PLUS_LENGTH_SIZE);
                breakFromAppend(dataInsideBlock, data, out);
            }
        } else if (msg instanceof SetupAppend) {
            breakFromAppend(null, null, out);
            writeMessage((SetupAppend) msg, out);
            SetupAppend setup = (SetupAppend) msg;
            setupSegments.put(new SimpleImmutableEntry<>(setup.getSegment(), setup.getWriterId()),
                              new Session(setup.getWriterId(), setup.getRequestId()));
        } else if (msg instanceof BlockTimeout) {
            BlockTimeout timeoutMsg = (BlockTimeout) msg;
            if (tokenCounter.get() == timeoutMsg.token) {
                breakFromAppend(null, null, out);
            }
        } else if (msg instanceof WireCommand) {
            breakFromAppend(null, null, out);
            writeMessage((WireCommand) msg, out);
        } else {
            throw new IllegalArgumentException("Expected a wire command and found: " + msg);
        }
    }

    private void validateAppend(Append append, Session session) {
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
        Preconditions.checkState(bytesLeftInBlock == 0 || bytesLeftInBlock > TYPE_PLUS_LENGTH_SIZE,
                "Bug in CommandEncoder.encode, block is too small.");
    }

    private void breakFromAppend(ByteBuf data, ByteBuf pendingData, ByteBuf out) {
        if (bytesLeftInBlock != 0) {
            if (data != null) {
                writeMessage(new PartialEvent(data), out);
            } else {
                writeMessage(new Padding(bytesLeftInBlock - TYPE_PLUS_LENGTH_SIZE), out);
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
        }
        segmentBeingAppendedTo = null;
        writerIdPerformingAppends = null;
    }

    @SneakyThrows(IOException.class)
    private void writeMessage(AppendBlock block, ByteBuf out) {
        int startIdx = out.writerIndex();
        ByteBufOutputStream bout = new ByteBufOutputStream(out);
        bout.writeInt(block.getType().getCode());
        bout.write(LENGTH_PLACEHOLDER);
        block.writeFields(bout);
        bout.flush();
        bout.close();
        int endIdx = out.writerIndex();
        int fieldsSize = endIdx - startIdx - TYPE_PLUS_LENGTH_SIZE;
        out.setInt(startIdx + TYPE_SIZE, fieldsSize + currentBlockSize);
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
