/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.nautilus.common.netty;

import static com.emc.nautilus.common.netty.WireCommands.APPEND_BLOCK_SIZE;

import java.io.IOException;
import java.util.UUID;

import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.SneakyThrows;

public class CommandEncoder extends MessageToByteEncoder<WireCommand> {
    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

    private UUID connectionId;
    private long connectionOffset;
    private boolean headerOnNextAppend;

    @Override
    protected void encode(ChannelHandlerContext ctx, WireCommand msg, ByteBuf out) throws Exception {
        switch (msg.getType()) {
        case APPEND_DATA:
            AppendData append = (AppendData) msg;
            if (connectionId == null || !connectionId.equals(append.getConnectionId())) {
                throw new InvalidMessageException("Sending appends without setting up the append.");
            }
            ByteBuf data = append.getData();
            int dataLength = data.readableBytes();
            if (append.getConnectionOffset() != connectionOffset + dataLength) {
                throw new InvalidMessageException("Connection offset in append not of expected length: "
                        + append.getConnectionOffset() + " vs " + (connectionOffset + dataLength));
            }
            if (headerOnNextAppend) {
                writeHeader(out);
                headerOnNextAppend = false;
            }

            long blockBoundry = getNextBlockBoundry();
            if (connectionOffset + dataLength < blockBoundry) {
                out.writeBytes(data, data.readerIndex(), dataLength);
                data.skipBytes(dataLength);
                connectionOffset += dataLength;
            } else {
                int inOldBlock = (int) (blockBoundry - connectionOffset);
                out.writeBytes(data, data.readerIndex(), inOldBlock);
                data.skipBytes(inOldBlock);
                connectionOffset += dataLength;
                if (append.getConnectionOffset() != connectionOffset) {
                    throw new IllegalStateException("Internal connectionOffset tracking error");
                }
                writeFooter(data, data.readableBytes(), out);
                headerOnNextAppend = true;
            }
            break;
        case SETUP_APPEND:
            breakFromAppend(out);
            writeNormalMessage(msg, out);
            connectionId = ((SetupAppend) msg).getConnectionId();
            break;
        default:
            breakFromAppend(out);
            writeNormalMessage(msg, out);
        }
    }

    private long getNextBlockBoundry() {
        return ((connectionOffset + APPEND_BLOCK_SIZE) / APPEND_BLOCK_SIZE) * APPEND_BLOCK_SIZE;
    }

    @SneakyThrows(IOException.class)
    private void writeHeader(ByteBuf out) {
        int startIdx = out.writerIndex();
        ByteBufOutputStream bout = new ByteBufOutputStream(out);
        bout.writeInt(WireCommandType.APPEND_DATA_HEADER.getCode());
        bout.write(LENGTH_PLACEHOLDER);
        bout.writeLong(connectionOffset);
        bout.writeLong(connectionId.getMostSignificantBits());
        bout.writeLong(connectionId.getLeastSignificantBits());
        bout.flush();
        bout.close();
        int endIdx = out.writerIndex();
        int nextBlockSize = (int) (getNextBlockBoundry() - connectionOffset);
        out.setInt(startIdx + 4, endIdx + nextBlockSize - startIdx - 8);
    }

    @SneakyThrows(IOException.class)
    private void writeFooter(ByteBuf data, int dataLength, ByteBuf out) {
        int startIdx = out.writerIndex();
        ByteBufOutputStream bout = new ByteBufOutputStream(out);
        bout.writeInt(WireCommandType.APPEND_DATA_FOOTER.getCode());
        bout.write(LENGTH_PLACEHOLDER);
        bout.writeLong(connectionOffset);
        bout.writeLong(connectionId.getMostSignificantBits());
        bout.writeLong(connectionId.getLeastSignificantBits());
        bout.writeInt(dataLength);
        bout.flush();
        bout.close();
        int endIdx = out.writerIndex();
        int dataWritten;
        if (data == null) {
            dataWritten = 0;
        } else {
            dataWritten = data.readableBytes();
            if (dataLength != dataWritten) {
                throw new IllegalStateException("Data length is wrong.");
            }
            out.writeBytes(data, data.readerIndex(), dataWritten);
            data.skipBytes(dataWritten);
        }
        out.setInt(startIdx + 4, endIdx + dataWritten - startIdx - 8);
    }

    private void breakFromAppend(ByteBuf out) throws IOException {
        if (!headerOnNextAppend) {
            long dataInBlock = connectionOffset - (connectionOffset / APPEND_BLOCK_SIZE) * APPEND_BLOCK_SIZE;
            if (dataInBlock > 0) {
                int remainingInBlock = (int) (APPEND_BLOCK_SIZE - dataInBlock);
                out.writeZero(remainingInBlock);
                writeFooter(null, -remainingInBlock, out);
            }
            headerOnNextAppend = true;
        }
        connectionId = null;
        connectionOffset = 0;
    }

    @SneakyThrows(IOException.class)
    private void writeNormalMessage(WireCommand msg, ByteBuf out) {
        int startIdx = out.writerIndex();
        ByteBufOutputStream bout = new ByteBufOutputStream(out);
        bout.writeInt(msg.getType().getCode());
        bout.write(LENGTH_PLACEHOLDER);
        msg.writeFields(bout);
        bout.flush();
        bout.close();
        int endIdx = out.writerIndex();
        out.setInt(startIdx + 4, endIdx - startIdx - 8);
    }

}
