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
package io.pravega.shared.protocol.netty;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import static io.netty.buffer.Unpooled.wrappedUnmodifiableBuffer;

/**
 * AppendBlocks are decoded specially to avoid having to parse every append individually.
 * This is done by holding onto the AppendBlock command and waiting for the following command
 * which must be an AppendBlockEnd.
 * The AppendBlockEnd command should have all of the information need to construct a single
 * Append object with all of the Events in the block.
 * <p>
 * The {@link #decode} method invokes {@link #processCommand(WireCommand)} to generate {@link Request} instances. Please
 * refer to {@link #processCommand(WireCommand)} Javadoc for important information about returned objects.
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
                || msg instanceof WireCommands.Padding || msg instanceof WireCommands.ConditionalAppend || msg instanceof  WireCommands.ConditionalBlockEnd;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, WireCommand command, List<Object> out) throws Exception {
        Object result = processCommand(command);
        if (result != null) {
            out.add(result);
        }
    }

    /**
     * Processes inbound {@link WireCommand} instances and converts them to {@link Request} instances.
     *
     * @param command Inbound {@link WireCommand}. Supported types are: {@link WireCommandType#PADDING},
     *                {@link WireCommandType#SETUP_APPEND}, {@link WireCommandType#CONDITIONAL_APPEND},
     *                {@link WireCommandType#APPEND_BLOCK} and {@link WireCommandType#APPEND_BLOCK_END},
     *                {@link WireCommandType#CONDITIONAL_BLOCK_END}.
     * @return One of the following:
     * - null if command type is {@link WireCommandType#PADDING} or {@link WireCommandType#APPEND_BLOCK}.
     * - command if command type is A {@link WireCommandType#SETUP_APPEND}.
     * - An {@link Append} instance for any other types. {@link Append#getData()} will be a {@link ByteBuf} that wraps
     * one or more {@link ByteBuf}s that are part of the supplied command instance(s) and will have a {@link ByteBuf#refCnt()}
     * at least equal to 1. It is very important that {@link Append#getData()}{@link ByteBuf#release()} is invoked on this
     * instance when it is no longer needed so that any direct memory used by it can be properly freed.
     * @throws InvalidMessageException If commands are received out of order or otherwise form an illegal Append.
     * @throws IOException             If unable to read from the command's data buffer.
     */
    @VisibleForTesting
    Request processCommand(WireCommand command) throws InvalidMessageException, IOException {
        try {
            return processCommandInternal(command);
        } catch (Throwable ex) {
            // Clean up.
            if (command instanceof WireCommands.ReleasableCommand) {
                // Release whatever data the current command holds on to.
                ((WireCommands.ReleasableCommand) command).release();
            }

            if (currentBlock != null) {
                // Release the current block we were holding on to.
                currentBlock.release();
                currentBlock = null;
            }
            throw ex;
        }
    }

    private Request processCommandInternal(WireCommand command) throws InvalidMessageException, IOException {
        if (currentBlock != null && command.getType() != WireCommandType.APPEND_BLOCK_END) {
            log.warn("Invalid message received {}. CurrentBlock {}", command, currentBlock);
            throw new InvalidMessageException("Unexpected " + command.getType() + " following a append block.");
        }
        Request result = null;
        switch (command.getType()) {
            case PADDING:
                // Nothing to do.
                break;
            case SETUP_APPEND:
                result = processSetupAppend((WireCommands.SetupAppend) command);
                break;
            case CONDITIONAL_APPEND:
                result = processConditionalAppend((WireCommands.ConditionalAppend) command);
                break;
            case APPEND_BLOCK:
                processAppendBlock((WireCommands.AppendBlock) command);
                break;
            case APPEND_BLOCK_END:
                result = processAppendBlockEnd((WireCommands.AppendBlockEnd) command);
                break;
            case CONDITIONAL_BLOCK_END:
                result = processConditionalBlockEnd((WireCommands.ConditionalBlockEnd) command);
                break;
            //$CASES-OMITTED$
            default:
                throw new IllegalStateException("Unexpected case: " + command);
        }
        return result;
    }

    private WireCommands.SetupAppend processSetupAppend(WireCommands.SetupAppend setup) {
        appendingSegments.put(setup.getWriterId(), new Segment(setup.getSegment()));
        return setup;
    }

    private Append processConditionalAppend(WireCommands.ConditionalAppend ca) {
        Segment segment = getSegment(ca.getWriterId());
        if (ca.getEventNumber() < segment.lastEventNumber) {
            throw new InvalidMessageException("Last event number went backwards.");
        }
        segment.lastEventNumber = ca.getEventNumber();
        return new Append(segment.getName(),
                ca.getWriterId(),
                ca.getEventNumber(),
                1,
                ca.getEvent().getAsByteBuf(),
                ca.getExpectedOffset(), ca.getRequestId());
    }

    private void processAppendBlock(WireCommands.AppendBlock appendBlock) {
        currentBlock = appendBlock;
        getSegment(currentBlock.getWriterId());
    }

    private Append processAppendBlockEnd(WireCommands.AppendBlockEnd blockEnd) throws InvalidMessageException, IOException {
        UUID writerId = blockEnd.getWriterId();
        Segment segment = getSegment(writerId);
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
        return new Append(segment.name, writerId, segment.lastEventNumber, blockEnd.numEvents, appendDataBuf, null, blockEnd.getRequestId());
    }

    private Append processConditionalBlockEnd(WireCommands.ConditionalBlockEnd ca) {
        Segment segment = getSegment(ca.getWriterId());
        if (ca.getEventNumber() < segment.lastEventNumber) {
            throw new InvalidMessageException("Last event number went backwards.");
        }
        segment.lastEventNumber = ca.getEventNumber();
        return new Append(segment.getName(),
                ca.getWriterId(),
                ca.getEventNumber(),
                1,
                ca.getData(),
                ca.getExpectedOffset(), ca.getRequestId());
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
                    currentBlock.release();
                    appendDataBuf = wrappedUnmodifiableBuffer(((WireCommands.PartialEvent) cmd).getData(), blockEnd.getData());
                } else {
                    appendDataBuf = wrappedUnmodifiableBuffer(appendDataBuf, ((WireCommands.PartialEvent) cmd).getData(), blockEnd.getData());
                }
            }
        }

        return appendDataBuf;
    }

    private Segment getSegment(UUID writerId) {
        Segment segment = appendingSegments.get(writerId);
        if (segment == null) {
            throw new InvalidMessageException("ConnectionID refrenced before SetupAppend");
        }
        return segment;
    }

}
