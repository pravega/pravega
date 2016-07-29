/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.nautilus.common.netty;

import static com.emc.nautilus.common.netty.WireCommandType.*;
import static com.emc.nautilus.common.netty.WireCommands.TYPE_PLUS_LENGTH_SIZE;
import static io.netty.buffer.Unpooled.wrappedBuffer;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import com.emc.nautilus.common.netty.WireCommands.Append;
import com.emc.nautilus.common.netty.WireCommands.AppendBlock;
import com.emc.nautilus.common.netty.WireCommands.AppendBlockEnd;
import com.emc.nautilus.common.netty.WireCommands.PartialEvent;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.Cleanup;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Decodes commands coming over the wire. For the most part this is just delegation to the
 * deserializers in WireCommands.
 * AppendBlocks are handled specially to avoid having to parse every append individually.
 * This is done by holding onto the AppendBlock command and waiting for the following command
 * which must be an AppendBlockEnd.
 * The AppendBlockEnd command should have all of the information need to construct a single
 * Append object with all of the Events in the block.
 * @See CommandEncoder For details about handling of PartialEvents
 */
@Slf4j
@ToString
public class CommandDecoder extends ByteToMessageDecoder {

    private final HashMap<UUID, Segment> appendingSegments = new HashMap<>();
    private AppendBlock currentBlock;

    @Data
    private static final class Segment {
        private final String name;
        private long lastEventNumber;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (log.isTraceEnabled()) {
            log.debug("Decoding a message on connection: " + this);
        }
        WireCommand command = parseCommand(in);
        command = processCommand(command);
        if (command != null) {
            out.add(command);
        }
    }

    @VisibleForTesting
    public WireCommand processCommand(WireCommand command) throws Exception {
        if (currentBlock != null && command.getType() != APPEND_BLOCK_END) {
            throw new InvalidMessageException("Unexpected " + command.getType() + " following a append block.");
        }
        switch (command.getType()) {
        case PADDING:
            return null;
        case SETUP_APPEND:
            SetupAppend append = (SetupAppend) command;
            appendingSegments.put(append.getConnectionId(), new Segment(append.getSegment()));
            return command;
        case APPEND_BLOCK:
            getSegment(((AppendBlock) command).getConnectionId());
            currentBlock = (AppendBlock) command;
            return null;
        case APPEND_BLOCK_END:
            AppendBlockEnd blockEnd = (AppendBlockEnd) command;
            if (currentBlock == null) {
                throw new InvalidMessageException("AppendBlockEnd without AppendBlock.");
            }
            UUID connectionId = blockEnd.getConnectionId();
            if (!connectionId.equals(currentBlock.getConnectionId())) {
                throw new InvalidMessageException("AppendBlockEnd for wrong connection.");
            }
            Segment segment = getSegment(connectionId);
            if (blockEnd.lastEventNumber < segment.lastEventNumber) {
                throw new InvalidMessageException("Last event number went backwards.");
            }
            int sizeOfWholeEventsInBlock = blockEnd.getSizeOfWholeEvents();
            if (sizeOfWholeEventsInBlock > currentBlock.getData().readableBytes() || sizeOfWholeEventsInBlock < 0) {
                throw new InvalidMessageException("Invalid SizeOfWholeEvents in block");
            }
            ByteBuf appendDataBuf = getAppendDataBuf(blockEnd, sizeOfWholeEventsInBlock);
            segment.lastEventNumber = blockEnd.lastEventNumber;
            currentBlock = null;
            return new Append(segment.name, connectionId, segment.lastEventNumber, appendDataBuf);
        default:
            return command;
        }
    }

    private ByteBuf getAppendDataBuf(AppendBlockEnd blockEnd, int sizeOfWholeEventsInBlock) throws IOException {
        ByteBuf appendDataBuf = currentBlock.getData().slice(0, sizeOfWholeEventsInBlock);
        int remaining = currentBlock.getData().readableBytes() - sizeOfWholeEventsInBlock;
        if (remaining > 0) {
            ByteBuf dataRemainingInBlock = currentBlock.getData().slice(sizeOfWholeEventsInBlock, remaining);
            WireCommand cmd = parseCommand(dataRemainingInBlock);
            if (!(cmd.getType() == PARTIAL_EVENT || cmd.getType() == PADDING)) {
                throw new InvalidMessageException("Found " + cmd.getType()
                        + " at end of append block but was expecting a partialEvent or padding.");
            }
            if (cmd.getType() == PADDING && blockEnd.getData().readableBytes() != 0) {
                throw new InvalidMessageException("Unexpected data in BlockEnd");
            }
            if (cmd.getType() == PARTIAL_EVENT) {
                // Work around a bug in netty:
                // See https://github.com/netty/netty/issues/5597
                if (appendDataBuf.readableBytes() == 0) {
                    appendDataBuf.release();
                    return wrappedBuffer(((PartialEvent) cmd).getData(), blockEnd.getData());
                } else {
                    return wrappedBuffer(appendDataBuf, ((PartialEvent) cmd).getData(), blockEnd.getData());
                }
            }
        }
        return appendDataBuf;
    }

    @VisibleForTesting
    public WireCommand parseCommand(ByteBuf in) throws IOException {
        @Cleanup
        ByteBufInputStream is = new ByteBufInputStream(in);
        int readableBytes = in.readableBytes();
        if (readableBytes < TYPE_PLUS_LENGTH_SIZE) {
            throw new InvalidMessageException("Not enough bytes to read.");
        }
        WireCommandType type = readType(is);
        int length = readLength(is, readableBytes);
        WireCommand command = type.readFrom(is, length);
        return command;
    }

    private Segment getSegment(UUID connectionId) {
        Segment segment = appendingSegments.get(connectionId);
        if (segment == null) {
            throw new InvalidMessageException("ConnectionID refrenced before SetupAppend");
        }
        return segment;
    }

    private int readLength(DataInput is, int readableBytes) throws IOException {
        int length = is.readInt();
        if (length < 0) {
            throw new InvalidMessageException("Length read from wire was negitive.");
        }
        if (length > readableBytes - TYPE_PLUS_LENGTH_SIZE) {
            throw new InvalidMessageException("Header indicated more bytes than exist.");
        }
        return length;
    }

    private WireCommandType readType(DataInput is) throws IOException {
        int t = is.readInt();
        WireCommandType type = WireCommands.getType(t);
        if (type == null) {
            throw new InvalidMessageException("Unknown wire command: " + t);
        }
        return type;
    }

}
