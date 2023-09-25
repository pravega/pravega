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
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.CorruptedFrameException;
import io.pravega.shared.segment.ScaleType;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.concurrent.NotThreadSafe;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.netty.buffer.Unpooled.wrappedBuffer;

/**
 * The complete list of all commands that go over the wire between clients and the server.
 * Each command is self-contained providing both it's serialization and deserialization logic.
 * Commands are not nested and contain only primitive types. The types are serialized in the obvious
 * way using Java's DataOutput and DataInput. All data is written BigEndian.
 *
 * Because length and type are detected externally these are not necessary for the classes to
 * supply.
 *
 * Compatible changes (i.e. Adding new members) that would not cause breakage if either the client or
 * the server were running older code can be made at any time.
 * Incompatible changes should instead create a new WireCommand object.
 */
public final class WireCommands {
    public static final int WIRE_VERSION = 17;
    public static final int OLDEST_COMPATIBLE_VERSION = 5;
    public static final int TYPE_SIZE = 4;
    public static final int TYPE_PLUS_LENGTH_SIZE = 8;
    public static final int MAX_WIRECOMMAND_SIZE = 0x00FFFFFF; // 16MB-1

    public static final long NULL_ATTRIBUTE_VALUE = Long.MIN_VALUE; //This is the same as Attributes.NULL_ATTRIBUTE_VALUE
    public static final long NULL_TABLE_SEGMENT_OFFSET = -1;

    private static final Map<Integer, WireCommandType> MAPPING;
    private static final String EMPTY_STACK_TRACE = "";
    static {
        HashMap<Integer, WireCommandType> map = new HashMap<>();
        for (WireCommandType t : WireCommandType.values()) {
            map.put(t.getCode(), t);
        }
        MAPPING = Collections.unmodifiableMap(map);
    }

    public static WireCommandType getType(int value) {
        return MAPPING.get(value);
    }

    @FunctionalInterface
    interface Constructor {
        WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException;
    }

    @Data
    public static final class Hello implements Request, Reply, WireCommand {
        final WireCommandType type = WireCommandType.HELLO;
        final int highVersion;
        final int lowVersion;

        @Override
        public void process(RequestProcessor cp) {
            cp.hello(this);
        }

        @Override
        public void process(ReplyProcessor cp) {
            cp.hello(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeInt(highVersion);
            out.writeInt(lowVersion);
        }

        public static Hello readFrom(DataInput in, int length) throws IOException {
            int highVersion = in.readInt();
            int lowVersion = in.readInt();
            return new Hello(highVersion, lowVersion);
        }

        @Override
        public long getRequestId() {
            return 0;
        }
    }

    @Data
    public static final class WrongHost implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.WRONG_HOST;
        final long requestId;
        final String segment;
        final String correctHost;
        final String serverStackTrace;

        @Override
        public void process(ReplyProcessor cp) {
            cp.wrongHost(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(correctHost);
            out.writeUTF(serverStackTrace);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String correctHost = in.readUTF();
            String serverStackTrace = (in.available() > 0) ? in.readUTF() : EMPTY_STACK_TRACE;
            return new WrongHost(requestId, segment, correctHost, serverStackTrace);
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Data
    public static final class SegmentIsSealed implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_IS_SEALED;
        final long requestId;
        final String segment;
        final String serverStackTrace;
        /**
         * This represents the offset at which the segment is sealed.
         */
        final long offset;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentIsSealed(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(serverStackTrace);
            out.writeLong(offset);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String serverStackTrace = (in.available() > 0) ? in.readUTF() : EMPTY_STACK_TRACE;
            long offset = (in.available() >= Long.BYTES) ? in.readLong() : -1L;
            return new SegmentIsSealed(requestId, segment, serverStackTrace, offset);
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Data
    public static final class SegmentIsTruncated implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_IS_TRUNCATED;
        final long requestId;
        final String segment;
        final long startOffset;
        final String serverStackTrace;
        /**
         * This represents the offset at which the segment is truncated.
         */
        final long offset;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentIsTruncated(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeLong(startOffset);
            out.writeUTF(serverStackTrace);
            out.writeLong(offset);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            long startOffset = in.readLong();
            String serverStackTrace = (in.available() > 0) ? in.readUTF() : EMPTY_STACK_TRACE;
            long offset = (in.available() >= Long.BYTES) ? in.readLong() : -1L;
            return new SegmentIsTruncated(requestId, segment, startOffset, serverStackTrace, offset);
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Data
    public static final class SegmentAlreadyExists implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_ALREADY_EXISTS;
        final long requestId;
        final String segment;
        final String serverStackTrace;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentAlreadyExists(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(serverStackTrace);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String serverStackTrace = (in.available() > 0) ? in.readUTF() : EMPTY_STACK_TRACE;
            return new SegmentAlreadyExists(requestId, segment, serverStackTrace);
        }

        @Override
        public String toString() {
            return "Segment already exists: " + segment;
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Data
    public static final class NoSuchSegment implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.NO_SUCH_SEGMENT;
        final long requestId;
        final String segment;
        final String serverStackTrace;
        /**
         * This represents the offset that was sent by the request.
         */
        final long offset;

        @Override
        public void process(ReplyProcessor cp) {
            cp.noSuchSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(serverStackTrace);
            out.writeLong(offset);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String serverStackTrace = (in.available() > 0) ? in.readUTF() : EMPTY_STACK_TRACE;
            long offset = (in.available() >= Long.BYTES) ? in.readLong() : -1L;
            return new NoSuchSegment(requestId, segment, serverStackTrace, offset);
        }

        @Override
        public String toString() {
            return "No such segment: " + segment;
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Data
    public static final class TableSegmentNotEmpty implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.TABLE_SEGMENT_NOT_EMPTY;
        final long requestId;
        final String segment;
        final String serverStackTrace;

        @Override
        public void process(ReplyProcessor cp) {
            cp.tableSegmentNotEmpty(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(serverStackTrace);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String serverStackTrace = in.readUTF();
            return new TableSegmentNotEmpty(requestId, segment, serverStackTrace);
        }

        @Override
        public String toString() {
            return "Table Segment is not empty: " + segment;
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Data
    @EqualsAndHashCode(exclude = "serverStackTrace")
    public static final class InvalidEventNumber implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.INVALID_EVENT_NUMBER;
        final UUID writerId;
        final long requestId;
        final String serverStackTrace;
        final long eventNumber;

        @Override
        public void process(ReplyProcessor cp) {
            cp.invalidEventNumber(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeLong(requestId);
            out.writeUTF(serverStackTrace);
            out.writeLong(eventNumber);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            UUID writerId = new UUID(in.readLong(), in.readLong());
            long requestId = in.readLong();
            String serverStackTrace = (in.available() > 0) ? in.readUTF() : EMPTY_STACK_TRACE;
            long eventNumber = (in.available() >= Long.BYTES) ? in.readLong() : -1L;
            return new InvalidEventNumber(writerId, requestId, serverStackTrace, eventNumber);
        }

        @Override
        public String toString() {
            return "Invalid event number: " + eventNumber + " for writer: " + writerId;
        }

        @Override
        public boolean isFailure() {
            return true;
        }

        @Override
        public long getRequestId() {
            return requestId;
        }
    }

    @Data
    public static final class OperationUnsupported implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.OPERATION_UNSUPPORTED;
        final long requestId;
        final String operationName;
        final String serverStackTrace;

        @Override
        public void process(ReplyProcessor cp) {
            cp.operationUnsupported(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(operationName);
            out.writeUTF(serverStackTrace);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String operationName = in.readUTF();
            String serverStackTrace = (in.available() > 0) ? in.readUTF() : EMPTY_STACK_TRACE;
            return new OperationUnsupported(requestId, operationName, serverStackTrace);
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Data
    public static final class Padding implements WireCommand {
        final WireCommandType type = WireCommandType.PADDING;
        final int length;

        Padding(int length) {
            Preconditions.checkArgument(length >= 0);
            this.length = length;
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            for (int i = 0; i < length / 8; i++) {
                out.writeLong(0L);
            }
            for (int i = 0; i < length % 8; i++) {
                out.writeByte(0);
            }
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            int skipped = 0;
            while (skipped < length) {
                int skipBytes = in.skipBytes(length - skipped);
                if (skipBytes < 0) {
                    throw new CorruptedFrameException("Not enough bytes in buffer. Was attempting to read: " + length);
                }
                skipped += skipBytes;
            }
            return new Padding(length);
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    public static final class PartialEvent extends ReleasableCommand {
        final WireCommandType type = WireCommandType.PARTIAL_EVENT;
        final ByteBuf data;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            data.getBytes(data.readerIndex(), (OutputStream) out, data.readableBytes());
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            return new PartialEvent(in.readFully(length).retain()).requireRelease();
        }

        @Override
        void releaseInternal() {
            this.data.release();
        }
    }

    @Data
    public static final class Event implements WireCommand {
        final WireCommandType type = WireCommandType.EVENT;
        final ByteBuf data;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeInt(type.getCode());
            out.writeInt(data.readableBytes());
            data.getBytes(data.readerIndex(), (OutputStream) out, data.readableBytes());
        }

        public ByteBuf getAsByteBuf() {
            ByteBuf header = Unpooled.buffer(TYPE_PLUS_LENGTH_SIZE, TYPE_PLUS_LENGTH_SIZE);
            header.writeInt(type.getCode());
            header.writeInt(data.readableBytes());
            return Unpooled.wrappedUnmodifiableBuffer(header, data);
        }
    }

    @Data
    public static final class SetupAppend implements Request, WireCommand {
        final WireCommandType type = WireCommandType.SETUP_APPEND;
        final long requestId;
        final UUID writerId;
        final String segment;
        @ToString.Exclude
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.setupAppend(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeUTF(segment);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            UUID uuid = new UUID(in.readLong(), in.readLong());
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            return new SetupAppend(requestId, uuid, segment, delegationToken);
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    public static final class AppendBlock extends ReleasableCommand {
        final WireCommandType type = WireCommandType.APPEND_BLOCK;
        final UUID writerId;
        final ByteBuf data;

        public AppendBlock(UUID writerId) {
            this.writerId = writerId;
            this.data = Unpooled.EMPTY_BUFFER; // Populated on read path
        }

        @VisibleForTesting
        public AppendBlock(UUID writerId, ByteBuf data) {
            this.writerId = writerId;
            this.data = data;
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            // Data not written, as it should be null.
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            UUID writerId = new UUID(in.readLong(), in.readLong());
            ByteBuf data = in.readFully(length - Long.BYTES * 2).retain();
            return new AppendBlock(writerId, data).requireRelease();
        }

        @Override
        void releaseInternal() {
            this.data.release();
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    public static final class AppendBlockEnd extends ReleasableCommand {
        final WireCommandType type = WireCommandType.APPEND_BLOCK_END;
        final UUID writerId;
        final int sizeOfWholeEvents;
        final ByteBuf data;
        final int numEvents;
        final long lastEventNumber;
        final long requestId;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeInt(sizeOfWholeEvents);
            if (data == null) {
                out.writeInt(0);
            } else {
                out.writeInt(data.readableBytes());
                data.getBytes(data.readerIndex(), (OutputStream) out, data.readableBytes());
            }
            out.writeInt(numEvents);
            out.writeLong(lastEventNumber);
            out.writeLong(requestId);
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            UUID writerId = new UUID(in.readLong(), in.readLong());
            int sizeOfHeaderlessAppends = in.readInt();
            int dataLength = in.readInt();
            ByteBuf data;
            if (dataLength > 0) {
                data = in.readFully(dataLength);
            } else {
                data = EMPTY_BUFFER;
            }
            int numEvents = in.readInt();
            long lastEventNumber = in.readLong();
            long requestId = in.available() >= Long.BYTES ? in.readLong() : -1L;
            return new AppendBlockEnd(writerId, sizeOfHeaderlessAppends, data.retain(), numEvents, lastEventNumber, requestId)
                    .requireRelease();
        }

        @Override
        void releaseInternal() {
            this.data.release();
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    public static final class ConditionalAppend extends ReleasableCommand implements Request {
        final WireCommandType type = WireCommandType.CONDITIONAL_APPEND;
        final UUID writerId;
        final long eventNumber;
        final long expectedOffset;
        final Event event;
        final long requestId;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeLong(eventNumber);
            out.writeLong(expectedOffset);
            event.writeFields(out);
            out.writeLong(requestId);
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            UUID writerId = new UUID(in.readLong(), in.readLong());
            long eventNumber = in.readLong();
            long expectedOffset = in.readLong();
            Event event = readEvent(in, length);
            long requestId = (in.available() >= Long.BYTES) ? in.readLong() : -1L;
            return new ConditionalAppend(writerId, eventNumber, expectedOffset, event, requestId).requireRelease();
        }

        private static Event readEvent(EnhancedByteBufInputStream in, int length) throws IOException {
            int typeCode = in.readInt();
            if (typeCode != WireCommandType.EVENT.getCode()) {
                throw new InvalidMessageException("Was expecting EVENT but found: " + typeCode);
            }
            int eventLength = in.readInt();
            if (eventLength > length - TYPE_PLUS_LENGTH_SIZE) {
                throw new InvalidMessageException("Was expecting length: " + length + " but found: " + eventLength);
            }
            return new Event(in.readFully(eventLength).retain());
        }

        @Override
        public long getRequestId() {
            return requestId;
        }

        @Override
        public void process(RequestProcessor cp) {
            //Unreachable. This should be handled in AppendDecoder.
            throw new UnsupportedOperationException();
        }

        @Override
        void releaseInternal() {
            this.event.data.release();
        }
    }

    @Data
    public static final class AppendSetup implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.APPEND_SETUP;
        final long requestId;
        final String segment;
        final UUID writerId;
        final long lastEventNumber;

        @Override
        public void process(ReplyProcessor cp) {
            cp.appendSetup(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeLong(lastEventNumber);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            UUID writerId = new UUID(in.readLong(), in.readLong());
            long lastEventNumber = in.readLong();
            return new AppendSetup(requestId, segment, writerId, lastEventNumber);
        }
    }

    @Data
    public static final class DataAppended implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.DATA_APPENDED;
        final long requestId;
        final UUID writerId;
        final long eventNumber;
        final long previousEventNumber;
        final long currentSegmentWriteOffset;

        @Override
        public void process(ReplyProcessor cp) {
            cp.dataAppended(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeLong(eventNumber);
            out.writeLong(previousEventNumber);
            out.writeLong(requestId);
            out.writeLong(currentSegmentWriteOffset);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            UUID writerId = new UUID(in.readLong(), in.readLong());
            long eventNumber = in.readLong();
            long previousEventNumber = in.available() >= Long.BYTES ? in.readLong() : -1L;
            long requestId = in.available() >= Long.BYTES ? in.readLong() : -1L;
            long currentSegmentWriteOffset = in.available() >= Long.BYTES ? in.readLong() : -1L;
            return new DataAppended(requestId, writerId, eventNumber, previousEventNumber, currentSegmentWriteOffset);
        }

        @Override
        public long getRequestId() {
            return requestId;
        }
    }

    @Data
    public static final class ConditionalCheckFailed implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.CONDITIONAL_CHECK_FAILED;
        final UUID writerId;
        final long eventNumber;
        final long requestId;

        @Override
        public void process(ReplyProcessor cp) {
            cp.conditionalCheckFailed(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeLong(eventNumber);
            out.writeLong(requestId);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            UUID writerId = new UUID(in.readLong(), in.readLong());
            long offset = in.readLong();
            long requestId = in.available() >= Long.BYTES ? in.readLong() : -1L;
            return new ConditionalCheckFailed(writerId, offset, requestId);
        }

        @Override
        public long getRequestId() {
            return requestId;
        }
    }

    @Data
    public static final class FlushToStorage implements Request, WireCommand {
        final WireCommandType type = WireCommandType.FLUSH_TO_STORAGE;
        final int containerId;
        @ToString.Exclude
        final String delegationToken;
        final long requestId;

        @Override
        public void process(RequestProcessor cp) {
            ((AdminRequestProcessor) cp).flushToStorage(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeInt(containerId);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
            out.writeLong(requestId);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            int containerId = in.readInt();
            String delegationToken = in.readUTF();
            long requestId = in.readLong();
            return new FlushToStorage(containerId, delegationToken, requestId);
        }
    }

    @Data
    public static final class StorageFlushed implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.FLUSHED_TO_STORAGE;
        final long requestId;

        @Override
        public void process(ReplyProcessor cp) {
            cp.storageFlushed(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            return new StorageFlushed(requestId);
        }
    }

    @Data
    public static final class ListStorageChunks implements Request, WireCommand {
        final WireCommandType type = WireCommandType.LIST_STORAGE_CHUNKS;
        final String segment;
        @ToString.Exclude
        final String delegationToken;
        final long requestId;

        @Override
        public void process(RequestProcessor cp) {
            ((AdminRequestProcessor) cp).listStorageChunks(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
            out.writeLong(requestId);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            long requestId = in.readLong();
            return new ListStorageChunks(segment, delegationToken, requestId);
        }
    }

    @Data
    public static final class StorageChunksListed implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.STORAGE_CHUNKS_LISTED;
        final long requestId;
        final List<ChunkInfo> chunks;

        @Override
        public void process(ReplyProcessor cp) {
            cp.storageChunksListed(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeInt(chunks.size());
            for (ChunkInfo chunk : chunks) {
                chunk.writeFields(out);
            }
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            int numberOfChunks = in.readInt();
            List<ChunkInfo> chunks = new ArrayList<>(numberOfChunks);
            for (int i = 0; i < numberOfChunks; i++) {
                chunks.add(ChunkInfo.readFrom(in, in.available()));
            }
            return new StorageChunksListed(requestId, chunks);
        }
    }

    @Data
    public static final class ChunkInfo {
        final long lengthInMetadata;
        final long lengthInStorage;
        final long startOffset;
        final String chunkName;
        final boolean existsInStorage;

        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(lengthInMetadata);
            out.writeLong(lengthInStorage);
            out.writeLong(startOffset);
            out.writeUTF(chunkName);
            out.writeBoolean(existsInStorage);
        }

        public static ChunkInfo readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            long lengthInMetadata = in.readLong();
            long lengthInStorage = in.readLong();
            long startOffset = in.readLong();
            String chunkName = in.readUTF();
            boolean existsInStorage = in.readBoolean();
            return new ChunkInfo(lengthInMetadata, lengthInStorage, startOffset, chunkName, existsInStorage);
        }
    }

    @Data
    public static final class ReadSegment implements Request, WireCommand {
        final WireCommandType type = WireCommandType.READ_SEGMENT;
        final String segment;
        final long offset;
        final int suggestedLength;
        @ToString.Exclude
        final String delegationToken;
        final long requestId;

        @Override
        public void process(RequestProcessor cp) {
            cp.readSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeLong(offset);
            out.writeInt(suggestedLength);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
            out.writeLong(requestId);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            String segment = in.readUTF();
            long offset = in.readLong();
            int suggestedLength = in.readInt();
            String delegationToken = in.readUTF();
            long requestId = in.available()  >= Long.BYTES ? in.readLong() : -1L;
            return new ReadSegment(segment, offset, suggestedLength, delegationToken, requestId);
        }

        @Override
        public long getRequestId() {
            return requestId;
        }
    }

    @RequiredArgsConstructor
    @Getter
    @ToString
    @EqualsAndHashCode(callSuper = false)
    @NotThreadSafe
    public static final class SegmentRead extends ReleasableCommand implements Reply {
        final WireCommandType type = WireCommandType.SEGMENT_READ;
        final String segment;
        final long offset;
        final boolean atTail; //TODO: Is sometimes false when actual state is unknown.
        final boolean endOfSegment;
        final ByteBuf data;
        final long requestId;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentRead(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeLong(offset);
            out.writeBoolean(atTail);
            out.writeBoolean(endOfSegment);
            int dataLength = data.readableBytes();
            out.writeInt(dataLength);
            this.data.getBytes(this.data.readerIndex(), (OutputStream) out, dataLength);
            out.writeLong(requestId);
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            String segment = in.readUTF();
            long offset = in.readLong();
            boolean atTail = in.readBoolean();
            boolean endOfSegment = in.readBoolean();
            int dataLength = in.readInt();
            if (dataLength > length) {
                throw new BufferOverflowException();
            }
            ByteBuf data = in.readFully(dataLength).retain();
            long requestId = in.available() >= Long.BYTES ? in.readLong() : -1L;
            return new SegmentRead(segment, offset, atTail, endOfSegment, data, requestId).requireRelease();
        }

        @Override
        void releaseInternal() {
            this.data.release();
        }

        @Override
        public long getRequestId() {
            return requestId;
        }
    }

    @Data
    public static final class GetSegmentAttribute implements Request, WireCommand {
        final WireCommandType type = WireCommandType.GET_SEGMENT_ATTRIBUTE;
        final long requestId;
        final String segmentName;
        final UUID attributeId;
        @ToString.Exclude
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.getSegmentAttribute(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segmentName);
            out.writeLong(attributeId.getMostSignificantBits());
            out.writeLong(attributeId.getLeastSignificantBits());
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            UUID attributeId = new UUID(in.readLong(), in.readLong());
            String delegationToken = in.readUTF();
            return new GetSegmentAttribute(requestId, segment, attributeId, delegationToken);
        }
    }

    @Data
    public static final class SegmentAttribute implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_ATTRIBUTE;
        final long requestId;
        final long value;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentAttribute(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeLong(value);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            long value = in.readLong();
            return new SegmentAttribute(requestId, value);
        }
    }

    @Data
    public static final class UpdateSegmentAttribute implements Request, WireCommand {
        final WireCommandType type = WireCommandType.UPDATE_SEGMENT_ATTRIBUTE;
        final long requestId;
        final String segmentName;
        final UUID attributeId;
        final long newValue;
        final long expectedValue;
        @ToString.Exclude
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.updateSegmentAttribute(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segmentName);
            out.writeLong(attributeId.getMostSignificantBits());
            out.writeLong(attributeId.getLeastSignificantBits());
            out.writeLong(newValue);
            out.writeLong(expectedValue);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            UUID attributeId = new UUID(in.readLong(), in.readLong());
            long newValue = in.readLong();
            long excpecteValue = in.readLong();
            String delegationToken = in.readUTF();
            return new UpdateSegmentAttribute(requestId, segment, attributeId, newValue, excpecteValue, delegationToken);
        }
    }

    @Data
    public static final class SegmentAttributeUpdated implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_ATTRIBUTE_UPDATED;
        final long requestId;
        final boolean success;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentAttributeUpdated(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeBoolean(success);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            boolean success = in.readBoolean();
            return new SegmentAttributeUpdated(requestId, success);
        }
    }

    @Data
    public static final class GetStreamSegmentInfo implements Request, WireCommand {
        final WireCommandType type = WireCommandType.GET_STREAM_SEGMENT_INFO;
        final long requestId;
        final String segmentName;
        @ToString.Exclude
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.getStreamSegmentInfo(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segmentName);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            return new GetStreamSegmentInfo(requestId, segment, delegationToken);
        }
    }

    @Data
    public static final class StreamSegmentInfo implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.STREAM_SEGMENT_INFO;
        final long requestId;
        final String segmentName;
        @Accessors(fluent = true)
        final boolean exists;
        final boolean isSealed;
        final boolean isDeleted;
        final long lastModified;
        final long writeOffset;
        final long startOffset;

        @Override
        public void process(ReplyProcessor cp) {
            cp.streamSegmentInfo(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segmentName);
            out.writeBoolean(exists);
            out.writeBoolean(isSealed);
            out.writeBoolean(isDeleted);
            out.writeLong(lastModified);
            out.writeLong(writeOffset);
            out.writeLong(startOffset);
        }

        public static <T extends InputStream & DataInput> WireCommand readFrom(T in, int length) throws IOException {
            long requestId = in.readLong();
            String segmentName = in.readUTF();
            boolean exists = in.readBoolean();
            boolean isSealed = in.readBoolean();
            boolean isDeleted = in.readBoolean();
            long lastModified = in.readLong();
            long segmentLength = in.readLong();
            long startOffset = 0;
            if (in.available() >= Long.BYTES) {
                // Versioning workaround until PDP-21 is implemented (https://github.com/pravega/pravega/issues/1948).
                startOffset = in.readLong();
            }
            return new StreamSegmentInfo(requestId, segmentName, exists, isSealed, isDeleted,
                                         lastModified, segmentLength, startOffset);
        }
    }

    @Data
    public static final class CreateSegment implements Request, WireCommand {
        public static final byte NO_SCALE = ScaleType.NoScaling.getValue();
        public static final byte IN_KBYTES_PER_SEC = ScaleType.Throughput.getValue();
        public static final byte IN_EVENTS_PER_SEC = ScaleType.EventRate.getValue();

        final WireCommandType type = WireCommandType.CREATE_SEGMENT;
        final long requestId;
        final String segment;
        final byte scaleType;
        final int targetRate;
        @ToString.Exclude
        final String delegationToken;
        final long rolloverSizeBytes;

        @Override
        public void process(RequestProcessor cp) {
            cp.createSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeInt(targetRate);
            out.writeByte(scaleType);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
            out.writeLong(rolloverSizeBytes);
        }

        public static <T extends InputStream & DataInput> WireCommand readFrom(T in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            int desiredRate = in.readInt();
            byte scaleType = in.readByte();
            String delegationToken = in.readUTF();
            long rolloverSizeBytes = 0;
            if (in.available() >= Long.BYTES) {
                rolloverSizeBytes = in.readLong();
            }

            return new CreateSegment(requestId, segment, scaleType, desiredRate, delegationToken, rolloverSizeBytes);
        }
    }

    @Data
    public static final class GetTableSegmentInfo implements Request, WireCommand {
        final WireCommandType type = WireCommandType.GET_TABLE_SEGMENT_INFO;
        final long requestId;
        final String segmentName;
        @ToString.Exclude
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.getTableSegmentInfo(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segmentName);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            return new GetTableSegmentInfo(requestId, segment, delegationToken);
        }
    }

    @Data
    public static final class TableSegmentInfo implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.TABLE_SEGMENT_INFO;
        final long requestId;
        final String segmentName;
        final long startOffset;
        final long length;
        final long entryCount;
        final int keyLength;

        @Override
        public void process(ReplyProcessor cp) {
            cp.tableSegmentInfo(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segmentName);
            out.writeLong(startOffset);
            out.writeLong(length);
            out.writeLong(entryCount);
            out.writeInt(keyLength);
        }

        public static <T extends InputStream & DataInput> WireCommand readFrom(T in, int length) throws IOException {
            long requestId = in.readLong();
            String segmentName = in.readUTF();
            long startOffset = in.readLong();
            long segmentLength = in.readLong();
            long entryCount = in.readLong();
            int keyLength = in.readInt();
            return new TableSegmentInfo(requestId, segmentName, startOffset, segmentLength, entryCount, keyLength);
        }
    }

    @Data
    public static final class CreateTableSegment implements Request, WireCommand {

        final WireCommandType type = WireCommandType.CREATE_TABLE_SEGMENT;
        final long requestId;
        final String segment;
        final boolean sortedDeprecated;
        final int keyLength;
        @ToString.Exclude
        final String delegationToken;
        final long rolloverSizeBytes;

        @Override
        public void process(RequestProcessor cp) {
            cp.createTableSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
            out.writeBoolean(sortedDeprecated);
            out.writeInt(keyLength);
            out.writeLong(rolloverSizeBytes);
        }

        public static <T extends InputStream & DataInput> WireCommand readFrom(T in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            boolean sorted = false;
            int keyLength = 0;
            long rolloverSizeBytes = 0;
            if (in.available() >= 1) {
                sorted = in.readBoolean();
            }
            if (in.available() >= Integer.BYTES) {
                keyLength = in.readInt();
            }
            if (in.available() >= Long.BYTES) {
                rolloverSizeBytes = in.readLong();
            }

            return new CreateTableSegment(requestId, segment, sorted, keyLength, delegationToken, rolloverSizeBytes);
        }
    }

    @Data
    public static final class SegmentCreated implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_CREATED;
        final long requestId;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentCreated(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            return new SegmentCreated(requestId, segment);
        }
    }

    @Data
    public static final class CreateTransientSegment implements Request, WireCommand {

        final WireCommandType type = WireCommandType.CREATE_TRANSIENT_SEGMENT;
        final long requestId;
        final UUID writerId;
        final String parentSegment;
        @ToString.Exclude
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.createTransientSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeUTF(parentSegment);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            UUID writerId = new UUID(in.readLong(), in.readLong());
            String parentSegment = in.readUTF();
            String delegationToken = in.readUTF();

            return new CreateTransientSegment(requestId, writerId, parentSegment, delegationToken);
        }
    }
    
    @Data
    public static final class LocateOffset implements Request, WireCommand {
        final WireCommandType type = WireCommandType.LOCATE_OFFSET;
        final long requestId;
        final String segment;
        final long targetOffset;
        @ToString.Exclude
        final String delegationToken;
        
        @Override
        public void process(RequestProcessor cp) {
            cp.locateOffset(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeLong(targetOffset);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            long targetOffset = in.readLong();
            String delegationToken = in.readUTF();
            return new LocateOffset(requestId, segment, targetOffset, delegationToken);
        }
    }
    
    @Data
    public static final class OffsetLocated implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.OFFSET_LOCATED;
        final long requestId;
        final String segment;
        final long offset;

        @Override
        public void process(ReplyProcessor cp) {
            cp.offsetLocated(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeLong(offset);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            long offset = in.readLong();
            return new OffsetLocated(requestId, segment, offset);
        }
    }

    @Data
    public static final class UpdateSegmentPolicy implements Request, WireCommand {

        final WireCommandType type = WireCommandType.UPDATE_SEGMENT_POLICY;
        final long requestId;
        final String segment;
        final byte scaleType;
        final int targetRate;
        @ToString.Exclude
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.updateSegmentPolicy(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeInt(targetRate);
            out.writeByte(scaleType);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            int desiredRate = in.readInt();
            byte scaleType = in.readByte();
            String delegationToken = in.readUTF();

            return new UpdateSegmentPolicy(requestId, segment, scaleType, desiredRate, delegationToken);
        }
    }

    @Data
    public static final class SegmentPolicyUpdated implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_POLICY_UPDATED;
        final long requestId;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentPolicyUpdated(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            return new SegmentPolicyUpdated(requestId, segment);
        }
    }

    @Data
    @AllArgsConstructor
    public static final class MergeSegments implements Request, WireCommand {
        final WireCommandType type = WireCommandType.MERGE_SEGMENTS;
        final long requestId;
        final String target;
        final String source;
        @ToString.Exclude
        final String delegationToken;
        final List<ConditionalAttributeUpdate> attributeUpdates;

        // Constructor to keep compatibility with all the calls not requiring attributes to merge Segments.
        public MergeSegments(long requestId, String target, String source, String delegationToken) {
            this.requestId = requestId;
            this.target = target;
            this.source = source;
            this.delegationToken = delegationToken;
            this.attributeUpdates = Collections.emptyList();
        }

        @Override
        public void process(RequestProcessor cp) {
            cp.mergeSegments(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(target);
            out.writeUTF(source);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
            out.writeInt(attributeUpdates.size());
            for (ConditionalAttributeUpdate entry : attributeUpdates) {
                entry.writeFields(out);
            }
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String target = in.readUTF();
            String source = in.readUTF();
            String delegationToken = in.readUTF();
            List<ConditionalAttributeUpdate> attributeUpdates = new ArrayList<>();
            if (in.available() <= 0) {
                // MergeSegment Commands prior v5 do not allow attributeUpdates, so we can return.
                return new MergeSegments(requestId, target, source, delegationToken, attributeUpdates);
            }
            int numberOfEntries = in.readInt();
            for (int i = 0; i < numberOfEntries; i++) {
                attributeUpdates.add(ConditionalAttributeUpdate.readFrom(in, length));
            }
            return new MergeSegments(requestId, target, source, delegationToken, attributeUpdates);
        }
    }

    @Data
    public static final class SegmentsMerged implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENTS_MERGED;
        final long requestId;
        final String target;
        final String source;
        final long newTargetWriteOffset;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentsMerged(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(target);
            out.writeUTF(source);
            out.writeLong(newTargetWriteOffset);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String target = in.readUTF();
            String source = in.readUTF();
            long newTargetWriteOffset = in.available() > 0 ? in.readLong() : -1;
            return new SegmentsMerged(requestId, target, source, newTargetWriteOffset);
        }
    }

    @Data
    public static final class MergeSegmentsBatch implements Request, WireCommand {
        final WireCommandType type = WireCommandType.MERGE_SEGMENTS_BATCH;
        final long requestId;
        final String targetSegmentId;
        final List<String> sourceSegmentIds;
        @ToString.Exclude
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.mergeSegmentsBatch(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(targetSegmentId);
            out.writeInt(sourceSegmentIds.size());
            for (int i = 0; i < sourceSegmentIds.size(); i++) {
                out.writeUTF(sourceSegmentIds.get(i));
            }
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String target = in.readUTF();
            int sourceCount = in.readInt();
            List<String> sources = new ArrayList<>(sourceCount);
            for (int i = 0; i < sourceCount; i++) {
                sources.add(in.readUTF());
            }
            String delegationToken = in.readUTF();
            return new MergeSegmentsBatch(requestId, target, sources, delegationToken);
        }
    }

    @Data
    public static final class SegmentsBatchMerged implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENTS_BATCH_MERGED;
        final long requestId;
        final String target;
        final List<String> sources;
        /** newTargetWriteOffset may not be the exact offset on the Segment post merge but just some offset following the merge.**/
        final List<Long> newTargetWriteOffset;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentsBatchMerged(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(target);
            out.writeInt(sources.size());
            for (int i = 0; i < sources.size(); i++) {
                out.writeUTF(sources.get(i));
                out.writeLong(newTargetWriteOffset.get(i));
            }
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String target = in.readUTF();
            int count = in.readInt();
            List<String> sources = new ArrayList<>(count);
            List<Long> offsets = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                sources.add(in.readUTF());
                offsets.add(in.readLong());
            }

            return new SegmentsBatchMerged(requestId, target, sources, offsets);
        }
    }

    @Data
    public static final class SealSegment implements Request, WireCommand {
        final WireCommandType type = WireCommandType.SEAL_SEGMENT;
        final long requestId;
        final String segment;
        @ToString.Exclude
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.sealSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            return new SealSegment(requestId, segment, delegationToken);
        }
    }

    @Data
    public static final class SegmentSealed implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_SEALED;
        final long requestId;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentSealed(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            return new SegmentSealed(requestId, segment);
        }
    }

    @Data
    public static final class TruncateSegment implements Request, WireCommand {
        final WireCommandType type = WireCommandType.TRUNCATE_SEGMENT;
        final long requestId;
        final String segment;
        final long truncationOffset;
        @ToString.Exclude
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.truncateSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeLong(truncationOffset);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            long truncationOffset = in.readLong();
            String delegationToken = in.readUTF();
            return new TruncateSegment(requestId, segment, truncationOffset, delegationToken);
        }
    }

    @Data
    public static final class SegmentTruncated implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_TRUNCATED;
        final long requestId;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentTruncated(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            return new SegmentTruncated(requestId, segment);
        }
    }

    @Data
    public static final class DeleteSegment implements Request, WireCommand {
        final WireCommandType type = WireCommandType.DELETE_SEGMENT;
        final long requestId;
        final String segment;
        @ToString.Exclude
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.deleteSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            return new DeleteSegment(requestId, segment, delegationToken);
        }
    }

    @Data
    public static final class DeleteTableSegment implements Request, WireCommand {
        final WireCommandType type = WireCommandType.DELETE_TABLE_SEGMENT;
        final long requestId;
        final String segment;
        final boolean mustBeEmpty; // If true, the Table Segment will only be deleted if it is empty (contains no keys)
        @ToString.Exclude
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.deleteTableSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeBoolean(mustBeEmpty);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            boolean mustBeEmpty = in.readBoolean();
            String delegationToken = in.readUTF();
            return new DeleteTableSegment(requestId, segment, mustBeEmpty, delegationToken);
        }
    }

    @Data
    public static final class SegmentDeleted implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_DELETED;
        final long requestId;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentDeleted(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            return new SegmentDeleted(requestId, segment);
        }
    }

    @Data
    public static final class KeepAlive implements Request, Reply, WireCommand {
        final WireCommandType type = WireCommandType.KEEP_ALIVE;

        @Override
        public void process(ReplyProcessor cp) {
            cp.keepAlive(this);
        }

        @Override
        public void process(RequestProcessor cp) {
            cp.keepAlive(this);
        }

        @Override
        public void writeFields(DataOutput out) {

        }

        public static WireCommand readFrom(DataInput in, int length) {
            return new KeepAlive();
        }

        @Override
        public long getRequestId() {
            return -1;
        }

        @Override
        public boolean mustLog() {
            return false;
        }
    }

    @Data
    public static final class AuthTokenCheckFailed implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.AUTH_TOKEN_CHECK_FAILED;
        final long requestId;
        final String serverStackTrace;
        final ErrorCode errorCode;

        public AuthTokenCheckFailed(long requestId, String serverStackTrace) {
            this(requestId, serverStackTrace, ErrorCode.UNSPECIFIED);
        }

        public AuthTokenCheckFailed(long requestId, String stackTrace, ErrorCode errorCode) {
            this.requestId = requestId;
            this.serverStackTrace = stackTrace;
            this.errorCode = errorCode;
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String serverStackTrace = (in.available() > 0) ? in.readUTF() : EMPTY_STACK_TRACE;

            // errorCode is a new field and wasn't present earlier. Doing this to allow it to work with older clients.
            int errorCode = in.available()  >= Integer.BYTES ? in.readInt() : -1;
            return new AuthTokenCheckFailed(requestId, serverStackTrace, ErrorCode.valueOf(errorCode));
        }

        public boolean isTokenExpired() {
            return errorCode == ErrorCode.TOKEN_EXPIRED;
        }
        
        @Override
        public boolean isFailure() {
            return true;
        }

        @Override
        public void process(ReplyProcessor cp) {
            cp.authTokenCheckFailed(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(serverStackTrace);
            out.writeInt(errorCode.getCode());
        }

        public enum ErrorCode {
            UNSPECIFIED(-1),       // indicates un-specified (for backward compatibility)
            TOKEN_CHECK_FAILED(0), // indicates a general token error
            TOKEN_EXPIRED(1);      // indicates token has expired

            private static final Map<Integer, ErrorCode> OBJECTS_BY_CODE = new HashMap<>();

            static {
                for (ErrorCode errorCode : ErrorCode.values()) {
                    OBJECTS_BY_CODE.put(errorCode.code, errorCode);
                }
            }

            private final int code;

            private ErrorCode(int code) {
                this.code = code;
            }

            public static ErrorCode valueOf(int code) {
                return OBJECTS_BY_CODE.getOrDefault(code, ErrorCode.TOKEN_CHECK_FAILED);
            }

            public int getCode() {
                return this.code;
            }
        }
    }

    /**
     * A generic error response that encapsulates an error code (to be used for client-side processing) and an error message
     * describing the origin of the error. This should be used to describe general exceptions where limited information is required.
     */
    @Data
    public static final class ErrorMessage implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.ERROR_MESSAGE;
        final long requestId;
        final String segment;
        final String message;
        final ErrorCode errorCode;

        @Override
        public void process(ReplyProcessor cp) throws UnsupportedOperationException {
            cp.errorMessage(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment == null ? "" : segment);
            out.writeUTF(message == null ? "" : message);
            out.writeInt(errorCode.getCode());
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            return new ErrorMessage(in.readLong(), in.readUTF(), in.readUTF(), ErrorCode.valueOf(in.readInt()));
        }

        public RuntimeException getThrowableException() {
            switch (errorCode) {
                case ILLEGAL_ARGUMENT_EXCEPTION:
                    return new IllegalArgumentException(message);
                default:
                    return new RuntimeException(message);
            }
        }

        @Override
        public boolean isFailure() {
            return true;
        }

        public enum ErrorCode {
            UNSPECIFIED(-1, RuntimeException.class),                       // indicates un-specified (for backward compatibility
            ILLEGAL_ARGUMENT_EXCEPTION(0, IllegalArgumentException.class); // indicates an IllegalArgumentException

            private static final Map<Integer, ErrorCode> OBJECTS_BY_CODE = new HashMap<>();
            private static final Map<Class, ErrorCode> OBJECTS_BY_CLASS = new HashMap<>();

            static {
                for (ErrorCode errorCode : ErrorCode.values()) {
                    OBJECTS_BY_CODE.put(errorCode.code, errorCode);
                    OBJECTS_BY_CLASS.put(errorCode.exception, errorCode);
                }
            }

            private final int code;
            private final Class<? extends Throwable> exception;

            private ErrorCode(int code, Class<? extends Exception> exception) {
                this.code = code;
                this.exception = exception;
            }

            public static ErrorCode valueOf(int code) {
                return OBJECTS_BY_CODE.getOrDefault(code, ErrorCode.UNSPECIFIED);
            }

            public static ErrorCode valueOf(Class<? extends Throwable> exception) {
                return OBJECTS_BY_CLASS.getOrDefault(exception, ErrorCode.UNSPECIFIED);
            }

            public int getCode() {
                return this.code;
            }

            public Class<? extends Throwable> getExceptionType() {
                return this.exception;
            }

        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    public static final class UpdateTableEntries extends ReleasableCommand implements Request, WireCommand {

        final WireCommandType type = WireCommandType.UPDATE_TABLE_ENTRIES;
        final long requestId;
        final String segment;
        @ToString.Exclude
        final String delegationToken;
        final TableEntries tableEntries;
        final long tableSegmentOffset;

        @Override
        public void process(RequestProcessor cp) {
            cp.updateTableEntries(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
            tableEntries.writeFields(out);
            out.writeLong(tableSegmentOffset);
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            TableEntries entries = TableEntries.readFrom(in, in.available());
            long tableSegmentOffset = (in.available() > 0 ) ? in.readLong() : NULL_TABLE_SEGMENT_OFFSET;

            return new UpdateTableEntries(requestId, segment, delegationToken, entries, tableSegmentOffset).requireRelease();
        }

        @Override
        void releaseInternal() {
            this.tableEntries.release();
        }
    }

    @Data
    public static final class TableEntriesUpdated implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.TABLE_ENTRIES_UPDATED;
        final long requestId;
        final List<Long> updatedVersions;

        @Override
        public void process(ReplyProcessor cp) {
            cp.tableEntriesUpdated(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeInt(updatedVersions.size());
            for (long version: updatedVersions) {
                out.writeLong(version);
            }
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            int numberOfEntries = in.readInt();
            List<Long> updatedVersions = new ArrayList<>(numberOfEntries);
            for (int i = 0; i < numberOfEntries; i++) {
                updatedVersions.add(in.readLong());
            }
            return new TableEntriesUpdated(requestId, updatedVersions);
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    public static final class RemoveTableKeys extends ReleasableCommand implements Request, WireCommand {

        final WireCommandType type = WireCommandType.REMOVE_TABLE_KEYS;
        final long requestId;
        final String segment;
        @ToString.Exclude
        final String delegationToken;
        final List<TableKey> keys;
        final long tableSegmentOffset;

        @Override
        public void process(RequestProcessor cp) {
            cp.removeTableKeys(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
            out.writeInt(keys.size());
            for (TableKey key : keys) {
                key.writeFields(out);
            }
            out.writeLong(tableSegmentOffset);
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            int numberOfKeys = in.readInt();
            List<TableKey> keys = new ArrayList<>(numberOfKeys);
            for (int i = 0; i < numberOfKeys; i++) {
                keys.add(TableKey.readFrom(in, in.available()));
            }
            long tableSegmentOffset = (in.available() > 0 ) ? in.readLong() : NULL_TABLE_SEGMENT_OFFSET;

            return new RemoveTableKeys(requestId, segment, delegationToken, keys, tableSegmentOffset).requireRelease();
        }

        @Override
        void releaseInternal() {
            this.keys.forEach(TableKey::release);
        }
    }

    @Data
    public static final class TableKeysRemoved implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.TABLE_KEYS_REMOVED;
        final long requestId;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {
            cp.tableKeysRemoved(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            return new TableKeysRemoved(requestId, segment);
        }
    }


    @Data
    @EqualsAndHashCode(callSuper = false)
    public static final class ReadTable extends ReleasableCommand implements Request, WireCommand {

        final WireCommandType type = WireCommandType.READ_TABLE;
        final long requestId;
        final String segment;
        @ToString.Exclude
        final String delegationToken;
        final List<TableKey> keys; // the version of the key is always set to io.pravega.segmentstore.contracts.tables.TableKey.NO_VERSION

        @Override
        public void process(RequestProcessor cp) {
            cp.readTable(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
            out.writeInt(keys.size());
            for (TableKey key : keys) {
                key.writeFields(out);
            }
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            int numberOfKeys = in.readInt();
            List<TableKey> keys = new ArrayList<>(numberOfKeys);
            for (int i = 0; i < numberOfKeys; i++) {
                keys.add(TableKey.readFrom(in, in.available()));
            }
            return new ReadTable(requestId, segment, delegationToken, keys).requireRelease();
        }

        @Override
        void releaseInternal() {
            this.keys.forEach(TableKey::release);
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    public static final class TableRead extends ReleasableCommand implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.TABLE_READ;
        final long requestId;
        final String segment;
        final TableEntries entries;

        @Override
        public void process(ReplyProcessor cp) {
            cp.tableRead(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            entries.writeFields(out);
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            TableEntries entries = TableEntries.readFrom(in, in.available());
            return new TableRead(requestId, segment, entries).requireRelease();
        }

        @Override
        void releaseInternal() {
            this.entries.release();
        }
    }

    @Data
    public static final class ReadTableKeys implements Request, WireCommand {

        final WireCommandType type = WireCommandType.READ_TABLE_KEYS;
        final long requestId;
        final String segment;
        @ToString.Exclude
        final String delegationToken;
        final int suggestedKeyCount;
        final TableIteratorArgs args;


        @Override
        public void process(RequestProcessor cp) {
            cp.readTableKeys(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
            out.writeInt(suggestedKeyCount);
            args.writeFields(out);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            int suggestedKeyCount = in.readInt();
            TableIteratorArgs args = new TableIteratorArgs(in);
            return new ReadTableKeys(requestId, segment, delegationToken, suggestedKeyCount, args);
        }
    }

    @RequiredArgsConstructor
    @Getter
    @EqualsAndHashCode
    public static final class TableIteratorArgs {
        final ByteBuf continuationToken; // Used to indicate the point from which the next entry should be fetched.
        final ByteBuf prefixFilter;      // (Deprecated as of 0.10) Used to indicate any prefix filters to apply to keys.
        final ByteBuf fromKey;           // Lower bound of the iteration.
        final ByteBuf toKey;             // Upper bound of the iteration.

        TableIteratorArgs(ByteBufInputStream in) throws IOException {
            this(readBuffer(in), readBuffer(in), readBuffer(in), readBuffer(in));
        }

        private static ByteBuf readBuffer(ByteBufInputStream in) throws IOException {
            int dataLength = in.available() >= Integer.BYTES ? in.readInt() : 0;
            byte[] data = new byte[dataLength];
            if (dataLength > 0) {
                in.readFully(data);
            }
            return wrappedBuffer(data);
        }

        void writeFields(DataOutput out) throws IOException {
            out.writeInt(continuationToken.readableBytes()); // continuation token length.
            if (continuationToken.readableBytes() != 0) {
                continuationToken.getBytes(continuationToken.readerIndex(), (OutputStream) out, continuationToken.readableBytes());
            }

            // Prefix Filter has been removed as of 0.10. For backwards compatibility, encoding a length of 0 here.
            out.writeInt(0);

            // FromKey/ToKey introduced in 0.10.
            out.writeInt(fromKey.readableBytes());
            if (fromKey.readableBytes() != 0) {
                fromKey.getBytes(fromKey.readerIndex(), (OutputStream) out, fromKey.readableBytes());
            }

            out.writeInt(toKey.readableBytes());
            if (toKey.readableBytes() != 0) {
                toKey.getBytes(toKey.readerIndex(), (OutputStream) out, toKey.readableBytes());
            }
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    public static final class TableKeysRead extends ReleasableCommand implements Reply, WireCommand {
        public static final int HEADER_BYTES = Long.BYTES;

        final WireCommandType type = WireCommandType.TABLE_KEYS_READ;
        final long requestId;
        final String segment;
        final List<TableKey> keys;
        final ByteBuf continuationToken; // this is used to indicate the point from which the next keys should be fetched.

        @Override
        public void process(ReplyProcessor cp) {
            cp.tableKeysRead(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeInt(keys.size());
            for (TableKey key : keys) {
                key.writeFields(out);
            }
            out.writeInt(continuationToken.readableBytes());
            continuationToken.getBytes(continuationToken.readerIndex(), (OutputStream) out, continuationToken.readableBytes());
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            final int initialAvailable = in.available();
            long requestId = in.readLong();
            String segment = in.readUTF();
            int numberOfKeys = in.readInt();
            List<TableKey> keys = new ArrayList<>(numberOfKeys);
            for (int i = 0; i < numberOfKeys; i++) {
                TableKey k = TableKey.readFrom(in, in.available());
                keys.add(k);
            }

            int dataLength = in.readInt();
            byte[] continuationToken = new byte[dataLength];
            in.readFully(continuationToken);

            return new TableKeysRead(requestId, segment, keys, wrappedBuffer(continuationToken)).requireRelease();
        }

        @Override
        void releaseInternal() {
            this.keys.forEach(TableKey::release);
        }
    }

    @Data
    public static final class ReadTableEntries implements Request, WireCommand {

        final WireCommandType type = WireCommandType.READ_TABLE_ENTRIES;
        final long requestId;
        final String segment;
        @ToString.Exclude
        final String delegationToken;
        final int suggestedEntryCount;
        final TableIteratorArgs args;

        @Override
        public void process(RequestProcessor cp) {
            cp.readTableEntries(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
            out.writeInt(suggestedEntryCount);
            args.writeFields(out);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            int suggestedEntryCount = in.readInt();
            TableIteratorArgs args = new TableIteratorArgs(in);
            return new ReadTableEntries(requestId, segment, delegationToken, suggestedEntryCount, args);
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    public static final class TableEntriesRead extends ReleasableCommand implements Reply, WireCommand {
        public static final int HEADER_BYTES = Long.BYTES;

        final WireCommandType type = WireCommandType.TABLE_ENTRIES_READ;
        final long requestId;
        final String segment;
        final TableEntries entries;
        final ByteBuf continuationToken; // this is used to indicate the point from which the next keys should be fetched.


        @Override
        public void process(ReplyProcessor cp) {
            cp.tableEntriesRead(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            entries.writeFields(out);
            out.writeInt(continuationToken.readableBytes());
            continuationToken.getBytes(continuationToken.readerIndex(), (OutputStream) out, continuationToken.readableBytes());
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            TableEntries entries = TableEntries.readFrom(in, in.available());
            int dataLength = in.readInt();
            byte[] continuationToken = new byte[dataLength];
            in.readFully(continuationToken);
            return new TableEntriesRead(requestId, segment, entries, wrappedBuffer(continuationToken)).requireRelease();
        }

        @Override
        void releaseInternal() {
            this.entries.release();
        }
    }

    @Data
    public static final class TableEntries {
        public static final int HEADER_BYTES = Integer.BYTES;

        final List<Map.Entry<TableKey, TableValue>> entries;

        public void writeFields(DataOutput out) throws IOException {
            out.writeInt(entries.size());
            for (Map.Entry<TableKey, TableValue> ent : entries) {
                ent.getKey().writeFields(out);
                ent.getValue().writeFields(out);
            }
        }

        public static TableEntries readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            int numberOfEntries = in.readInt();
            List<Map.Entry<TableKey, TableValue>> entries = new ArrayList<>();
            for (int i = 0; i < numberOfEntries; i++) {
                entries.add(new AbstractMap.SimpleImmutableEntry<>(TableKey.readFrom(in, in.available()),
                                                                   TableValue.readFrom(in, in.available())));
            }

            return new TableEntries(entries);
        }

        void release() {
            this.entries.forEach(e -> {
                e.getKey().release();
                e.getValue().release();
            });
        }
    }

    @Data
    public static final class TableKey {
        public static final long NO_VERSION = Long.MIN_VALUE;
        public static final long NOT_EXISTS = -1L;
        public static final int HEADER_BYTES = 2 * Integer.BYTES;
        public static final TableKey EMPTY = new TableKey(EMPTY_BUFFER, Long.MIN_VALUE);

        final ByteBuf data;
        final long keyVersion;

        public void writeFields(DataOutput out) throws IOException {
            out.writeInt(Integer.BYTES + data.readableBytes() + Long.BYTES); // total length of the TableKey.
            out.writeInt(data.readableBytes()); // data length.
            if (data.readableBytes() != 0) {
                data.getBytes(data.readerIndex(), (OutputStream) out, data.readableBytes());
                out.writeLong(keyVersion);
            }
        }

        public static TableKey readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            int payLoadSize = in.readInt();
            int dataLength = in.readInt();
            if (dataLength == 0) {
                return TableKey.EMPTY;
            }
            if (length < payLoadSize) {
                throw new InvalidMessageException("Was expecting length of at least : " + payLoadSize + " but found: " + length);
            }

            ByteBuf msg = in.readFully(dataLength);
            long keyVersion = in.readLong();
            return new TableKey(msg.retain(), keyVersion);
        }

        public int size() {
            return HEADER_BYTES + this.data.readableBytes() + Long.BYTES;
        }

        void release() {
            this.data.release();
        }
    }

    @Data
    public static final class TableValue {
        public static final TableValue EMPTY = new TableValue(EMPTY_BUFFER);
        public static final int HEADER_BYTES = 2 * Integer.BYTES;

        final ByteBuf data;

        public void writeFields(DataOutput out) throws IOException {
            out.writeInt(Integer.BYTES + data.readableBytes()); // total length of of TableValue.
            out.writeInt(data.readableBytes()); // data length.
            if (data.readableBytes() != 0) {
                data.getBytes(data.readerIndex(), (OutputStream) out, data.readableBytes());
            }
        }

        public static TableValue readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            int payloadSize = in.readInt();
            int valueLength = in.readInt();
            if (valueLength == 0) {
                return TableValue.EMPTY;
            }
            if (length < payloadSize) {
                throw new InvalidMessageException("Was expecting length of at least : " + payloadSize + " but found: " + length);
            }

            ByteBuf msg = in.readFully(valueLength);
            return new TableValue(msg.retain());
        }

        public int size() {
            return HEADER_BYTES + this.data.readableBytes();
        }

        void release() {
            this.data.release();
        }
    }

    @Data
    public static final class TableKeyDoesNotExist implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.TABLE_KEY_DOES_NOT_EXIST;
        final long requestId;
        final String segment;
        final String serverStackTrace;

        @Override
        public void process(ReplyProcessor cp) {
            cp.tableKeyDoesNotExist(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(serverStackTrace);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String serverStackTrace = in.readUTF();
            return new TableKeyDoesNotExist(requestId, segment, serverStackTrace);
        }

        @Override
        public String toString() {
            return "Conditional table update failed since the key does not exist : " + segment;
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Data
    public static final class TableKeyBadVersion implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.TABLE_KEY_BAD_VERSION;
        final long requestId;
        final String segment;
        final String serverStackTrace;

        @Override
        public void process(ReplyProcessor cp) {
            cp.tableKeyBadVersion(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(serverStackTrace);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String serverStackTrace = in.readUTF();
            return new TableKeyBadVersion(requestId, segment, serverStackTrace);
        }

        @Override
        public String toString() {
            return "Conditional table update failed since the key version is incorrect : " + segment;
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Data
    public static final class ReadTableEntriesDelta implements Request, WireCommand {
        final WireCommandType type = WireCommandType.READ_TABLE_ENTRIES_DELTA;
        final long requestId;
        final String segment;
        @ToString.Exclude
        final String delegationToken;
        final long fromPosition;
        final int suggestedEntryCount;

        @Override
        public void process(RequestProcessor cp) {
            cp.readTableEntriesDelta(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
            out.writeLong(fromPosition);
            out.writeInt(suggestedEntryCount);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            long fromPosition = in.readLong();
            int suggestedEntryCount = in.readInt();

            return new ReadTableEntriesDelta(requestId, segment, delegationToken, fromPosition, suggestedEntryCount);
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    public static final class TableEntriesDeltaRead extends ReleasableCommand implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.TABLE_ENTRIES_DELTA_READ;
        final long requestId;
        final String segment;
        final TableEntries entries;
        final boolean shouldClear;
        final boolean reachedEnd;
        final long lastPosition;

        @Override
        public void process(ReplyProcessor cp) throws UnsupportedOperationException {
            cp.tableEntriesDeltaRead(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            entries.writeFields(out);
            out.writeBoolean(shouldClear);
            out.writeBoolean(reachedEnd);
            out.writeLong(lastPosition);
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            TableEntries entries = TableEntries.readFrom(in, in.available());
            boolean shouldClear = in.readBoolean();
            boolean reachedEnd = in.readBoolean();
            long lastPosition = in.readLong();

            return new TableEntriesDeltaRead(requestId, segment, entries, shouldClear, reachedEnd, lastPosition).requireRelease();
        }

        @Override
        void releaseInternal() {
            this.entries.release();
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    public static final class ConditionalBlockEnd extends ReleasableCommand implements Request {
        final WireCommandType type = WireCommandType.CONDITIONAL_BLOCK_END;
        final UUID writerId;
        final long eventNumber;
        final long expectedOffset;
        final ByteBuf data;
        final long requestId;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeLong(eventNumber);
            out.writeLong(expectedOffset);
            if (data == null) {
                out.writeInt(0);
            } else {
                out.writeInt(data.readableBytes());
                data.getBytes(data.readerIndex(), (OutputStream) out, data.readableBytes());
            }
            out.writeLong(requestId);
        }

        public static WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
            UUID writerId = new UUID(in.readLong(), in.readLong());
            long eventNumber = in.readLong();
            long expectedOffset = in.readLong();
            int dataLength = in.readInt();
            ByteBuf data;
            if (dataLength > 0) {
                data = in.readFully(dataLength);
            } else {
                data = EMPTY_BUFFER;
            }
            long requestId = in.readLong();
            return new ConditionalBlockEnd(writerId, eventNumber, expectedOffset, data.retain(), requestId).requireRelease();
        }

        @Override
        public long getRequestId() {
            return requestId;
        }

        @Override
        public void process(RequestProcessor cp) {
            //Unreachable. This should be handled in AppendDecoder.
            throw new UnsupportedOperationException();
        }

        @Override
        void releaseInternal() {
            this.data.release();
        }
    }

    /**
     * Base class for any command that may require releasing resources.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static abstract class ReleasableCommand implements WireCommand {
        @Getter
        private boolean released = true;

        /**
         * Marks the fact that this instance requires {@link #release()} to be invoked in order to free up resources.
         *
         * @return This instance.
         */
        WireCommand requireRelease() {
            this.released = false;
            return this;
        }

        /**
         * Releases any resources used by this command, if needed {@code #isReleased()} is false. This method has no
         * effect if invoked multiple times or if no resource release is required.
         */
        public void release() {
            if (!this.released) {
                releaseInternal();
                this.released = true;
            }
        }

        /**
         * Internal implementation of {@link #release()}. Do not invoke directly as this method offers no protection
         * against multiple invocations.
         */
        abstract void releaseInternal();
    }

    /**
     * Convenience class to encapsulate the contents of an attribute update when several should be serialized in the same
     * WireCommand.
     */
    @Data
    public static final class ConditionalAttributeUpdate {
        public static final byte REPLACE = (byte) 1; // AttributeUpdate of type AttributeUpdateType.Replace.
        public static final byte REPLACE_IF_EQUALS = (byte) 4; // AttributeUpdate of type AttributeUpdateType.ReplaceIfEquals.
        public static final int LENGTH = 4 * Long.BYTES + 1; // UUID (2 longs) + oldValue + newValue + updateType (1 byte)

        private final UUID attributeId;
        private final byte attributeUpdateType;
        private final long newValue;
        private final long oldValue;

        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(attributeId.getMostSignificantBits());
            out.writeLong(attributeId.getLeastSignificantBits());
            out.writeByte(attributeUpdateType);
            out.writeLong(newValue);
            out.writeLong(oldValue);
        }

        public static ConditionalAttributeUpdate readFrom(DataInput in, int length) throws IOException {
            UUID attributeId = new UUID(in.readLong(), in.readLong());
            byte attributeUpdateType = in.readByte();
            long newValue = in.readLong();
            long oldValue = in.readLong();
            return new ConditionalAttributeUpdate(attributeId, attributeUpdateType, newValue, oldValue);
        }

        public int size() {
            return LENGTH;
        }
    }
}
