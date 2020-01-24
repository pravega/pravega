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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.util.ByteArraySegment;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import static io.netty.buffer.Unpooled.wrappedBuffer;

/**
 * AppendBlocks are decoded specially to avoid having to parse every append individually.
 * This is done by holding onto the AppendBlock command and waiting for the following command
 * which must be an AppendBlockEnd.
 * The AppendBlockEnd command should have all of the information need to construct a single
 * Append object with all of the Events in the block.
 *
 * @see CommandEncoder For details about handling of PartialEvents
 */
@Slf4j
public class AppendDecoder extends MessageToMessageDecoder<WireCommand> {

    private final HashMap<UUID, Segment> appendingSegments = new HashMap<>();
    private WireCommands.AppendBlock currentBlock;

    @Data
    private static final class Segment {
        private final String name;
        private long lastEventNumber;
    }

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        return msg instanceof WireCommands.SetupAppend || msg instanceof WireCommands.AppendBlock || msg instanceof WireCommands.AppendBlockEnd
                || msg instanceof WireCommands.Padding || msg instanceof WireCommands.ConditionalAppend;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, WireCommand command, List<Object> out) throws Exception {
        Object result = processCommand(command);
        if (result != null) {
            out.add(result);
        }
    }

    @VisibleForTesting
    public Request processCommand(WireCommand command) throws Exception {
        if (currentBlock != null && command.getType() != WireCommandType.APPEND_BLOCK_END) {
            log.warn("Invalid message received {}. CurrentBlock {}", command, currentBlock);
            throw new InvalidMessageException("Unexpected " + command.getType() + " following a append block.");
        }
        Request result;
        Segment segment;
        switch (command.getType()) {
        case PADDING:
            result = null;
            break;
        case SETUP_APPEND:
            WireCommands.SetupAppend append = (WireCommands.SetupAppend) command;
            appendingSegments.put(append.getWriterId(), new Segment(append.getSegment()));
            result = append;
            break;
        case CONDITIONAL_APPEND:
            WireCommands.ConditionalAppend ca = (WireCommands.ConditionalAppend) command;
            segment = getSegment(ca.getWriterId());
            if (ca.getEventNumber() < segment.lastEventNumber) {
                throw new InvalidMessageException("Last event number went backwards.");
            }
            segment.lastEventNumber = ca.getEventNumber();
            EnhancedByteArrayOutputStream bout = new EnhancedByteArrayOutputStream();
            ca.getEvent().writeFields(new DataOutputStream(bout));
            ByteArraySegment data = bout.getData();
            result = new Append(segment.getName(),
                    ca.getWriterId(),
                    ca.getEventNumber(),
                    1,
                    Unpooled.wrappedBuffer(data.array(), data.arrayOffset(), data.getLength()),
                    ca.getExpectedOffset(), ca.getRequestId());
            break;
        case APPEND_BLOCK:
            currentBlock = (WireCommands.AppendBlock) command;
            getSegment(currentBlock.getWriterId());
            result = null;
            break;
        case APPEND_BLOCK_END:
            WireCommands.AppendBlockEnd blockEnd = (WireCommands.AppendBlockEnd) command;
            UUID writerId = blockEnd.getWriterId();
            segment = getSegment(writerId);
            int sizeOfWholeEventsInBlock = blockEnd.getSizeOfWholeEvents();
            ByteBuf appendDataBuf;
            if (blockEnd.numEvents <= 0) {
                throw new InvalidMessageException("Invalid number of events in block. numEvents : " + blockEnd.numEvents);
            }
            if (blockEnd.getLastEventNumber() < segment.lastEventNumber) {
                throw new InvalidMessageException(
                        String.format("Last event number went backwards, " +
                                "Segment last Event number : %d , Append block End Event number : %d," +
                                "for writer ID: %s and Segment Name: %s", segment.lastEventNumber,
                                blockEnd.getLastEventNumber(), writerId, segment.name));
            }
            if (currentBlock != null) {
               if (!currentBlock.getWriterId().equals(writerId)) {
                   throw new InvalidMessageException(
                           String.format("Writer ID mismatch between Append Block and Append block End, " +
                                   "Append block Writer ID : %s, Append block End Writer ID: %s",
                                   currentBlock.getWriterId(), writerId));
               }
               if (sizeOfWholeEventsInBlock > currentBlock.getData().readableBytes() || sizeOfWholeEventsInBlock < 0) {
                    throw new InvalidMessageException(
                            String.format("Invalid SizeOfWholeEvents in block : %d, Append block data bytes : %d",
                                    sizeOfWholeEventsInBlock, currentBlock.getData().readableBytes()));
               }
               appendDataBuf = getAppendDataBuf(blockEnd, sizeOfWholeEventsInBlock);
            } else {
               appendDataBuf = blockEnd.getData();
            }
            if (appendDataBuf == null) {
               throw new InvalidMessageException("Invalid data in block");
            }
            segment.lastEventNumber = blockEnd.getLastEventNumber();
            currentBlock = null;
            result = new Append(segment.name, writerId, segment.lastEventNumber, blockEnd.numEvents, appendDataBuf, null, blockEnd.getRequestId());
            break;
            //$CASES-OMITTED$
        default:
            throw new IllegalStateException("Unexpected case: " + command);
        }
        return result;
    }

    private ByteBuf getAppendDataBuf(WireCommands.AppendBlockEnd blockEnd, int sizeOfWholeEventsInBlock) throws IOException {
        ByteBuf appendDataBuf = currentBlock.getData().slice(0, sizeOfWholeEventsInBlock);
        final int remaining = currentBlock.getData().readableBytes() - sizeOfWholeEventsInBlock;
        if (remaining > 0) {
            ByteBuf dataRemainingInBlock = currentBlock.getData().slice(sizeOfWholeEventsInBlock, remaining);
            WireCommand cmd = CommandDecoder.parseCommand(dataRemainingInBlock);
            if (!(cmd.getType() == WireCommandType.PARTIAL_EVENT || cmd.getType() == WireCommandType.PADDING)) {
                throw new InvalidMessageException("Found " + cmd.getType()
                        + " at end of append block but was expecting a partialEvent or padding.");
            }
            if (cmd.getType() == WireCommandType.PADDING && blockEnd.getData().readableBytes() != 0) {
                throw new InvalidMessageException("Unexpected data in BlockEnd");
            }
            if (cmd.getType() == WireCommandType.PARTIAL_EVENT) {
                // Work around a bug in netty:
                // See https://github.com/netty/netty/issues/5597
                if (appendDataBuf.readableBytes() == 0) {
                    appendDataBuf.release();
                    appendDataBuf = wrappedBuffer(((WireCommands.PartialEvent) cmd).getData(), blockEnd.getData());
                } else {
                    appendDataBuf = wrappedBuffer(appendDataBuf, ((WireCommands.PartialEvent) cmd).getData(), blockEnd.getData());
                }
            }
        }

        // Make a copy of the ByteBuf as the readable bytes of the result may be significantly less than the total allocated
        // memory for this buffer. This problem is exacerbated with connection pooling where we may have a good amount of
        // padding around each append. Since this Append ByteBuf is expected to live for as long as the Segment Store
        // needs to process it, we need to compact this so that we don't use an excessive amount of heap memory which could
        // lead to out-of-memory situations.
        ByteBuf result = appendDataBuf.copy();
        appendDataBuf.release();
        return result;
    }

    private Segment getSegment(UUID writerId) {
        Segment segment = appendingSegments.get(writerId);
        if (segment == null) {
            throw new InvalidMessageException("ConnectionID refrenced before SetupAppend");
        }
        return segment;
    }

}
