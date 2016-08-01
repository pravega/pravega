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

import static com.emc.nautilus.common.netty.WireCommands.*;
import static io.netty.buffer.Unpooled.wrappedBuffer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.UUID;

import com.emc.nautilus.common.netty.WireCommands.Append;
import com.emc.nautilus.common.netty.WireCommands.AppendBlock;
import com.emc.nautilus.common.netty.WireCommands.AppendBlockEnd;
import com.emc.nautilus.common.netty.WireCommands.Event;
import com.emc.nautilus.common.netty.WireCommands.Padding;
import com.emc.nautilus.common.netty.WireCommands.PartialEvent;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.google.common.base.Preconditions;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.Data;
import lombok.SneakyThrows;

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
public class CommandEncoder extends MessageToByteEncoder<WireCommand> {
    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

    private final HashMap<String, Session> setupSegments = new HashMap<>();
    private String segmentBeingAppendedTo;
    private int bytesLeftInBlock;

    @Data
    private static final class Session {
        private final UUID id;
        private long lastEventNumber = -1L;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, WireCommand msg, ByteBuf out) throws Exception {
        if (msg instanceof Append) {
            Append append = (Append) msg;
            Session session = setupSegments.get(append.segment);
            if (session == null || !session.id.equals(append.getConnectionId())) {
                throw new InvalidMessageException("Sending appends without setting up the append.");
            }
            if (append.getEventNumber() <= session.lastEventNumber) {
                throw new InvalidMessageException("Events written out of order. Received: " + append.getEventNumber()
                        + " following: " + session.lastEventNumber);
            }
            Preconditions.checkState(bytesLeftInBlock == 0 || bytesLeftInBlock > TYPE_PLUS_LENGTH_SIZE,
                                     "Bug in CommandEncoder.encode, block is too small.");
            if (append.segment != segmentBeingAppendedTo) {
                breakFromAppend(out);
            }
            if (bytesLeftInBlock == 0) {
                writeMessage(new AppendBlock(session.id), out);
                bytesLeftInBlock = APPEND_BLOCK_SIZE;
                segmentBeingAppendedTo = append.segment;
            }

            session.lastEventNumber = append.getEventNumber();

            ByteBuf data = append.getData();
            int msgSize = TYPE_PLUS_LENGTH_SIZE + data.readableBytes();
            // Is there enough space for a subsequent message after this one?
            if (bytesLeftInBlock - msgSize > TYPE_PLUS_LENGTH_SIZE) {
                bytesLeftInBlock -= writeMessage(new Event(data), out);
            } else {
                byte[] serializedMessage = serializeMessage(new Event(data));
                int bytesInBlock = bytesLeftInBlock - TYPE_PLUS_LENGTH_SIZE;
                ByteBuf dataInsideBlock = wrappedBuffer(serializedMessage, 0, bytesInBlock);
                ByteBuf dataRemainging = wrappedBuffer(serializedMessage,
                                                       bytesInBlock,
                                                       serializedMessage.length - bytesInBlock);
                writeMessage(new PartialEvent(dataInsideBlock), out);
                writeMessage(new AppendBlockEnd(session.id,
                        session.lastEventNumber,
                        APPEND_BLOCK_SIZE - bytesLeftInBlock,
                        dataRemainging), out);
                bytesLeftInBlock = 0;
            }
        } else if (msg instanceof SetupAppend) {
            breakFromAppend(out);
            writeMessage(msg, out);
            SetupAppend setup = (SetupAppend) msg;
            setupSegments.put(setup.getSegment(), new Session(setup.getConnectionId()));
        } else {
            breakFromAppend(out);
            writeMessage(msg, out);
        }
    }

    private void breakFromAppend(ByteBuf out) {
        if (bytesLeftInBlock != 0) {
            writeMessage(new Padding(bytesLeftInBlock - TYPE_PLUS_LENGTH_SIZE), out);
            Session session = setupSegments.get(segmentBeingAppendedTo);
            writeMessage(new AppendBlockEnd(session.id,
                    session.lastEventNumber,
                    APPEND_BLOCK_SIZE - bytesLeftInBlock,
                    null), out);
            bytesLeftInBlock = 0;
        }
        segmentBeingAppendedTo = null;
    }

    @SneakyThrows(IOException.class)
    private byte[] serializeMessage(WireCommand msg) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bout);
        out.writeInt(msg.getType().getCode());
        out.write(LENGTH_PLACEHOLDER);
        msg.writeFields(out);
        out.flush();
        out.close();
        byte[] result = bout.toByteArray();
        ByteBuffer asBuffer = ByteBuffer.wrap(result);
        asBuffer.putInt(TYPE_SIZE, result.length - TYPE_PLUS_LENGTH_SIZE);
        return result;
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
        out.setInt(startIdx + TYPE_SIZE, fieldsSize + APPEND_BLOCK_SIZE);
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

}
