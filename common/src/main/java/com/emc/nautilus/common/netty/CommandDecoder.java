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

import java.util.List;
import java.util.UUID;

import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.common.netty.WireCommands.Type;
import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommandDecoder extends ByteToMessageDecoder {

    private UUID connectionId;
    private long connectionOffset;
    private ByteBuf appendHeader;

    public CommandDecoder() {
        connectionId = null;
        connectionOffset = 0;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        log.debug("Decoding a message");
        decode(in, out);
    }

    @VisibleForTesting
    public void decode(ByteBuf in, List<Object> out) throws Exception {
        @Cleanup
        ByteBufInputStream is = new ByteBufInputStream(in);
        int t = is.readInt();
        Type type = WireCommands.getType(t);
        int length = is.readInt();
        if (length > in.readableBytes()) {
            throw new IllegalStateException("Header indicated more bytes than exist.");
        }
        int dataStart = in.readerIndex();
        if (type == null) {
            throw new IllegalStateException("Unknown wire command: " + t);
        }
        switch (type) {
        case APPEND_DATA_HEADER: {
            long readOffset = is.readLong();
            UUID id = new UUID(in.readLong(), in.readLong());
            checkConnectionId(id);
            checkOffset(connectionOffset, readOffset);
            if (appendHeader != null) {
                throw new IllegalStateException("Header appended immediatly after header.");
            }
            appendHeader = in.readBytes(length - (in.readerIndex() - dataStart));
            break;
        }
        case APPEND_DATA_FOOTER: {
            long readOffset = is.readLong();
            UUID id = new UUID(in.readLong(), in.readLong());
            checkConnectionId(id);
            int dataLength = is.readInt();
            if (appendHeader == null) {
                throw new IllegalStateException("Footer not following header.");
            }
            long appendLength = appendHeader.readableBytes() + dataLength;
            connectionOffset += appendLength;
            checkOffset(connectionOffset, readOffset);
            if (dataLength > 0) {
                ByteBuf footerData = in.readBytes(dataLength);
                out.add(new AppendData(connectionId,
                        connectionOffset,
                        Unpooled.wrappedBuffer(appendHeader, footerData)));
            } else {
                int offset = appendHeader.writerIndex();
                appendHeader.writerIndex(offset + dataLength);
                out.add(new AppendData(connectionId, connectionOffset, appendHeader));
            }
            appendHeader = null;
            break;
        }
        case SETUP_APPEND: {
            SetupAppend setupAppend = (SetupAppend) type.readFrom(is);
            connectionId = setupAppend.getConnectionId();
            connectionOffset = 0;
            out.add(setupAppend);
            break;
        }
        default:
            out.add(type.readFrom(is));
            break;
        }
    }

    private void checkOffset(long expectedOffset, long readOffset) {
        if (expectedOffset != readOffset) {
            throw new IllegalStateException("Append came in at wrong offset: " + expectedOffset + " vs " + readOffset);
        }
    }

    private void checkConnectionId(UUID id) {
        if (id == null || !id.equals(connectionId)) {
            throw new IllegalStateException("Append came in for a segment that was not the appending segment.");
        }
    }

}
