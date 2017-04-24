/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common.netty;

import static io.netty.buffer.Unpooled.wrappedBuffer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

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
            appendingSegments.put(append.getConnectionId(), new Segment(append.getSegment()));
            result = append;
            break;
        case CONDITIONAL_APPEND:
            WireCommands.ConditionalAppend ca = (WireCommands.ConditionalAppend) command;
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
            getSegment(((WireCommands.AppendBlock) command).getConnectionId());
            currentBlock = (WireCommands.AppendBlock) command;
            result = null;
            break;
        case APPEND_BLOCK_END:
            WireCommands.AppendBlockEnd blockEnd = (WireCommands.AppendBlockEnd) command;
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
            //$CASES-OMITTED$
        default:
            throw new IllegalStateException("Unexpected case: " + command);
        }
        return result;
    }
    
    private ByteBuf getAppendDataBuf(WireCommands.AppendBlockEnd blockEnd, int sizeOfWholeEventsInBlock) throws IOException {
        ByteBuf appendDataBuf = currentBlock.getData().slice(0, sizeOfWholeEventsInBlock);
        int remaining = currentBlock.getData().readableBytes() - sizeOfWholeEventsInBlock;
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
                    return wrappedBuffer(((WireCommands.PartialEvent) cmd).getData(), blockEnd.getData());
                } else {
                    return wrappedBuffer(appendDataBuf, ((WireCommands.PartialEvent) cmd).getData(), blockEnd.getData());
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
