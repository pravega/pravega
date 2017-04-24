/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common.netty;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.Cleanup;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Decodes commands coming over the wire. For the most part this is just delegation to the
 * deserializers in WireCommands.
 */
@Slf4j
@ToString
public class CommandDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        WireCommand command = parseCommand(in);
        if (log.isTraceEnabled()) {
            log.trace("Decode a message on connection: {}. Message was {}", ctx.channel().remoteAddress(), command );
        }
        if (command != null) {
            out.add(command);
        }
    }

    @VisibleForTesting
    public static WireCommand parseCommand(ByteBuf in) throws IOException {
        @Cleanup
        ByteBufInputStream is = new ByteBufInputStream(in);
        int readableBytes = in.readableBytes();
        if (readableBytes < WireCommands.TYPE_PLUS_LENGTH_SIZE) {
            throw new InvalidMessageException("Not enough bytes to read.");
        }
        WireCommandType type = readType(is);
        int length = readLength(is, readableBytes);
        WireCommand command = type.readFrom(is, length);
        return command;
    }

    private static int readLength(DataInput is, int readableBytes) throws IOException {
        int length = is.readInt();
        if (length < 0) {
            throw new InvalidMessageException("Length read from wire was negitive.");
        }
        if (length > readableBytes - WireCommands.TYPE_PLUS_LENGTH_SIZE) {
            throw new InvalidMessageException("Header indicated more bytes than exist.");
        }
        return length;
    }

    private static WireCommandType readType(DataInput is) throws IOException {
        int t = is.readInt();
        WireCommandType type = WireCommands.getType(t);
        if (type == null) {
            throw new InvalidMessageException("Unknown wire command: " + t);
        }
        return type;
    }

}
