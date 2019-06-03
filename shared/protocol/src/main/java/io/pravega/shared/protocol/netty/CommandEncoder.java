/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Data;
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

    private final AppendBatchSizeTracker blockSizeSupplier;
    private final Map<Map.Entry<String, UUID>, Session> setupSegments = new HashMap<>();
    private String segmentBeingAppendedTo;
    private UUID writerIdPerformingAppends;
    private int currentBlockSize;
    private int bytesLeftInBlock;

    @Data
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
            Session session = setupSegments.get(new SimpleImmutableEntry<>(append.segment, append.getWriterId()));
            validateAppend(append, session);
            if (!append.segment.equals(segmentBeingAppendedTo) || !append.getWriterId().equals(writerIdPerformingAppends)) {
                breakFromAppend(out);
            }
            if (bytesLeftInBlock == 0) {
                currentBlockSize = Math.max(TYPE_PLUS_LENGTH_SIZE, blockSizeSupplier.getAppendBlockSize());
                bytesLeftInBlock = currentBlockSize;
                segmentBeingAppendedTo = append.segment;
                writerIdPerformingAppends = append.writerId;
                writeMessage(new AppendBlock(session.id), out);
                if (ctx != null) {
                    ctx.executor().schedule(new BlockTimeouter(ctx.channel(), currentBlockSize),
                                            blockSizeSupplier.getBatchTimeout(),
                                            TimeUnit.MILLISECONDS);
                }
            }

            session.lastEventNumber = append.getEventNumber();
            session.eventCount++;
            ByteBuf data = append.getData().slice();
            int msgSize = data.readableBytes();
            // Is there enough space for a subsequent message after this one?
            if (bytesLeftInBlock - msgSize > TYPE_PLUS_LENGTH_SIZE) {
                out.writeBytes(data);
                bytesLeftInBlock -= msgSize;
            } else {
                int bytesInBlock = bytesLeftInBlock - TYPE_PLUS_LENGTH_SIZE; 
                ByteBuf dataInsideBlock = data.readSlice(bytesInBlock);
                ByteBuf dataRemainging = data;
                writeMessage(new PartialEvent(dataInsideBlock), out);
                writeMessage(new AppendBlockEnd(append.writerId,
                                                currentBlockSize - bytesLeftInBlock,
                                                dataRemainging,
                                                session.eventCount,
                                                session.lastEventNumber,
                                                append.getRequestId()), out);
                bytesLeftInBlock = 0;
                session.eventCount = 0;
            }
        } else if (msg instanceof SetupAppend) {
            breakFromAppend(out);
            writeMessage((SetupAppend) msg, out);
            SetupAppend setup = (SetupAppend) msg;
            setupSegments.put(new SimpleImmutableEntry<>(setup.getSegment(), setup.getWriterId()),
                              new Session(setup.getWriterId(), setup.getRequestId()));
        } else if (msg instanceof BlockTimeout) {
            BlockTimeout timeoutMsg = (BlockTimeout) msg;
            if (currentBlockSize == timeoutMsg.ifStillBlockSize) {
                breakFromAppend(out);
            }
        } else if (msg instanceof WireCommand) {
            breakFromAppend(out);
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

    private void breakFromAppend(ByteBuf out) {
        if (bytesLeftInBlock != 0) {
            writeMessage(new Padding(bytesLeftInBlock - TYPE_PLUS_LENGTH_SIZE), out);
            Session session = setupSegments.get(new SimpleImmutableEntry<>(segmentBeingAppendedTo, writerIdPerformingAppends));

            writeMessage(new AppendBlockEnd(session.id,
                    currentBlockSize - bytesLeftInBlock,
                    null,
                    session.eventCount,
                    session.lastEventNumber, session.requestId), out);
            bytesLeftInBlock = 0;
            currentBlockSize = 0;
            session.eventCount = 0;
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
        private final int ifStillBlockSize;
    }
    
    @RequiredArgsConstructor
    private static final class BlockTimeouter implements Runnable {
        private final Channel channel;
        private final int blockSize;

        @Override
        public void run() {
            channel.writeAndFlush(new BlockTimeout(blockSize));
        }
    }

}
