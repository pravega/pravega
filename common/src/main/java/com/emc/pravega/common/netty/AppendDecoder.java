/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.common.netty;

import static com.emc.pravega.common.netty.WireCommandType.APPEND_BLOCK_END;
import static com.emc.pravega.common.netty.WireCommandType.PADDING;
import static com.emc.pravega.common.netty.WireCommandType.PARTIAL_EVENT;
import static io.netty.buffer.Unpooled.wrappedBuffer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import com.emc.pravega.common.netty.WireCommands.AppendBlock;
import com.emc.pravega.common.netty.WireCommands.AppendBlockEnd;
import com.emc.pravega.common.netty.WireCommands.ConditionalAppend;
import com.emc.pravega.common.netty.WireCommands.Padding;
import com.emc.pravega.common.netty.WireCommands.PartialEvent;
import com.emc.pravega.common.netty.WireCommands.SetupAppend;
import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import lombok.Data;

/**
 * AppendBlocks are decoded specially to avoid having to parse every append individually.
 * This is done by holding onto the AppendBlock command and waiting for the following command
 * which must be an AppendBlockEnd.
 * The AppendBlockEnd command should have all of the information need to construct a single
 * Append object with all of the Events in the block.
 * 
 * @see CommandEncoder For details about handling of PartialEvents
 */
public class AppendDecoder extends MessageToMessageDecoder<WireCommand> {

    private final HashMap<UUID, Segment> appendingSegments = new HashMap<>();
    private AppendBlock currentBlock;

    @Data
    private static final class Segment {
        private final String name;
        private long lastEventNumber;
    }
    
    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        return msg instanceof SetupAppend || msg instanceof AppendBlock || msg instanceof AppendBlockEnd
                || msg instanceof Padding || msg instanceof ConditionalAppend;
    }
    
    @Override
    protected void decode(ChannelHandlerContext ctx, WireCommand command, List<Object> out) throws Exception {
        Object result = processCommand(command);
        if (result != null) {
            out.add(result);
        }
    }

    /**
     * Parses wire command and creates append request object.
     *
     * @param command Append Wire Command
     * @return Append Instance.
     * @throws Exception thrown if command type is not append, or event block is constructed properly.
     */
    @VisibleForTesting
    public Request processCommand(WireCommand command) throws Exception {
        if (currentBlock != null && command.getType() != APPEND_BLOCK_END) {
            throw new InvalidMessageException("Unexpected " + command.getType() + " following a append block.");
        }
        Request result;
        Segment segment;
        switch (command.getType()) {
        case PADDING:
            result = null;
            break;
        case SETUP_APPEND:
            SetupAppend append = (SetupAppend) command;
            appendingSegments.put(append.getConnectionId(), new Segment(append.getSegment()));
            result = append;
            break;
        case CONDITIONAL_APPEND:
            ConditionalAppend ca = (ConditionalAppend) command;
            segment = getSegment(ca.getConnectionId());
            if (ca.getEventNumber() < segment.lastEventNumber) {
                throw new InvalidMessageException("Last event number went backwards.");
            }
            segment.lastEventNumber = ca.getEventNumber();
            result = new Append(segment.getName(),
                    ca.getConnectionId(),
                    ca.getEventNumber(),
                    ca.getData(),
                    ca.getExpectedOffset());
            break;
        case APPEND_BLOCK:
            getSegment(((AppendBlock) command).getConnectionId());
            currentBlock = (AppendBlock) command;
            result = null;
            break;
        case APPEND_BLOCK_END:
            AppendBlockEnd blockEnd = (AppendBlockEnd) command;
            if (currentBlock == null) {
                throw new InvalidMessageException("AppendBlockEnd without AppendBlock.");
            }
            UUID connectionId = blockEnd.getConnectionId();
            if (!connectionId.equals(currentBlock.getConnectionId())) {
                throw new InvalidMessageException("AppendBlockEnd for wrong connection.");
            }
            segment = getSegment(connectionId);
            if (blockEnd.getLastEventNumber() < segment.lastEventNumber) {
                throw new InvalidMessageException("Last event number went backwards.");
            }
            int sizeOfWholeEventsInBlock = blockEnd.getSizeOfWholeEvents();
            if (sizeOfWholeEventsInBlock > currentBlock.getData().readableBytes() || sizeOfWholeEventsInBlock < 0) {
                throw new InvalidMessageException("Invalid SizeOfWholeEvents in block");
            }
            ByteBuf appendDataBuf = getAppendDataBuf(blockEnd, sizeOfWholeEventsInBlock);
            segment.lastEventNumber = blockEnd.getLastEventNumber();
            currentBlock = null;
            result = new Append(segment.name, connectionId, segment.lastEventNumber, appendDataBuf, null);
            break;
        default:
            throw new IllegalStateException("Unexpected case: " + command);
        }
        return result;
    }
    
    private ByteBuf getAppendDataBuf(AppendBlockEnd blockEnd, int sizeOfWholeEventsInBlock) throws IOException {
        ByteBuf appendDataBuf = currentBlock.getData().slice(0, sizeOfWholeEventsInBlock);
        int remaining = currentBlock.getData().readableBytes() - sizeOfWholeEventsInBlock;
        if (remaining > 0) {
            ByteBuf dataRemainingInBlock = currentBlock.getData().slice(sizeOfWholeEventsInBlock, remaining);
            WireCommand cmd = CommandDecoder.parseCommand(dataRemainingInBlock);
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
    
    private Segment getSegment(UUID connectionId) {
        Segment segment = appendingSegments.get(connectionId);
        if (segment == null) {
            throw new InvalidMessageException("ConnectionID refrenced before SetupAppend");
        }
        return segment;
    }

}
