/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.common.netty;

import static io.netty.buffer.Unpooled.wrappedBuffer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Preconditions;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.experimental.Accessors;

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
    public static final int TYPE_SIZE = 4;
    public static final int TYPE_PLUS_LENGTH_SIZE = 8;
    public static final int MAX_WIRECOMMAND_SIZE = 0x007FFFFF; // 8MB
    private static final Map<Integer, WireCommandType> MAPPING;
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
        WireCommand readFrom(DataInput in, int length) throws IOException;
    }

    @Data
    public static final class WrongHost implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.WRONG_HOST;
        final String segment;
        final String correctHost;

        @Override
        public void process(ReplyProcessor cp) {
            cp.wrongHost(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeUTF(correctHost);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            String correctHost = in.readUTF();
            return new WrongHost(segment, correctHost);
        }
    }

    @Data
    public static final class SegmentIsSealed implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_IS_SEALED;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentIsSealed(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            return new SegmentIsSealed(segment);
        }
    }

    @Data
    public static final class SegmentAlreadyExists implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_ALREADY_EXISTS;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentAlreadyExists(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            return new SegmentAlreadyExists(segment);
        }

        @Override
        public String toString() {
            return "Segment already exists: " + segment;
        }
    }

    @Data
    public static final class NoSuchSegment implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.NO_SUCH_SEGMENT;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {
            cp.noSuchSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            return new NoSuchSegment(segment);
        }

        @Override
        public String toString() {
            return "No such segment: " + segment;
        }
    }

    @Data
    public static final class NoSuchTransaction implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.NO_SUCH_TRANSACTION;
        final String batch;

        @Override
        public void process(ReplyProcessor cp) {
            cp.noSuchBatch(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(batch);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String batch = in.readUTF();
            return new NoSuchTransaction(batch);
        }

        @Override
        public String toString() {
            return "No such batch: " + batch;
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
            in.skipBytes(length);
            return new Padding(length);
        }
    }

    @Data
    public static final class PartialEvent implements WireCommand {
        final WireCommandType type = WireCommandType.PARTIAL_EVENT;
        final ByteBuf data;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.write(data.array(), data.arrayOffset(), data.readableBytes());
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            byte[] msg = new byte[length];
            in.readFully(msg);
            return new PartialEvent(wrappedBuffer(msg));
        }
    }

    @Data
    public static final class Event implements WireCommand {
        final WireCommandType type = WireCommandType.EVENT;
        final ByteBuf data;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.write(data.array(), data.arrayOffset(), data.readableBytes());
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            byte[] msg = new byte[length];
            in.readFully(msg);
            return new Event(Unpooled.wrappedBuffer(msg));
        }
    }

    @Data
    public static final class SetupAppend implements Request, WireCommand {
        final WireCommandType type = WireCommandType.SETUP_APPEND;
        final UUID connectionId;
        final String segment;

        @Override
        public void process(RequestProcessor cp) {
            cp.setupAppend(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(connectionId.getMostSignificantBits());
            out.writeLong(connectionId.getLeastSignificantBits());
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            UUID uuid = new UUID(in.readLong(), in.readLong());
            String segment = in.readUTF();
            return new SetupAppend(uuid, segment);
        }
    }

    @Data
    public static final class AppendBlock implements WireCommand {
        final WireCommandType type = WireCommandType.APPEND_BLOCK;
        final UUID connectionId;
        final ByteBuf data;

        AppendBlock(UUID connectionId) {
            this.connectionId = connectionId;
            this.data = null; // Populated on read path
        }

        AppendBlock(UUID connectionId, ByteBuf data) {
            this.connectionId = connectionId;
            this.data = data;
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(connectionId.getMostSignificantBits());
            out.writeLong(connectionId.getLeastSignificantBits());
            // Data not written, as it should be null.
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            UUID connectionId = new UUID(in.readLong(), in.readLong());
            byte[] data = new byte[length - 16];
            in.readFully(data);
            return new AppendBlock(connectionId, wrappedBuffer(data));
        }
    }

    @Data
    public static final class AppendBlockEnd implements WireCommand {
        final WireCommandType type = WireCommandType.APPEND_BLOCK_END;
        final UUID connectionId;
        final long lastEventNumber;
        final int sizeOfWholeEvents;
        final ByteBuf data;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(connectionId.getMostSignificantBits());
            out.writeLong(connectionId.getLeastSignificantBits());
            out.writeLong(lastEventNumber);
            out.writeInt(sizeOfWholeEvents);
            if (data == null) {
                out.writeInt(0);
            } else {
                out.writeInt(data.readableBytes());
                out.write(data.array(), data.arrayOffset(), data.readableBytes());
            }
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            UUID connectionId = new UUID(in.readLong(), in.readLong());
            long lastEventNumber = in.readLong();
            int sizeOfHeaderlessAppends = in.readInt();
            int dataLength = in.readInt();
            byte[] data;
            if (dataLength > 0) {
                data = new byte[dataLength];
                in.readFully(data);
            } else {
                data = new byte[0];
            }
            return new AppendBlockEnd(connectionId, lastEventNumber, sizeOfHeaderlessAppends, wrappedBuffer(data));
        }
    }
    
    @Data
    public static final class ConditionalAppend implements WireCommand {
        final WireCommandType type = WireCommandType.CONDITIONAL_APPEND;
        final UUID connectionId;
        final long eventNumber;
        final long expectedOffset;
        final ByteBuf data;
        

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(connectionId.getMostSignificantBits());
            out.writeLong(connectionId.getLeastSignificantBits());
            out.writeLong(eventNumber);
            out.writeLong(expectedOffset);
            if (data == null) {
                out.writeInt(0);
            } else {
                out.writeInt(data.readableBytes());
                out.write(data.array(), data.arrayOffset(), data.readableBytes());
            }
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            UUID connectionId = new UUID(in.readLong(), in.readLong());
            long eventNumber = in.readLong();
            long expectedOffset = in.readLong();
            int dataLength = in.readInt();
            byte[] data;
            if (dataLength > 0) {
                data = new byte[dataLength];
                in.readFully(data);
            } else {
                data = new byte[0];
            }
            return new ConditionalAppend(connectionId, eventNumber, expectedOffset, wrappedBuffer(data));
        }
    }
    

    @Data
    public static final class AppendSetup implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.APPEND_SETUP;
        final String segment;
        final UUID connectionId;
        final long lastEventNumber;

        @Override
        public void process(ReplyProcessor cp) {
            cp.appendSetup(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeLong(connectionId.getMostSignificantBits());
            out.writeLong(connectionId.getLeastSignificantBits());
            out.writeLong(lastEventNumber);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            UUID connectionId = new UUID(in.readLong(), in.readLong());
            long lastEventNumber = in.readLong();
            return new AppendSetup(segment, connectionId, lastEventNumber);
        }
    }

    @Data
    public static final class DataAppended implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.DATA_APPENDED;
        final UUID connectionId;
        final long eventNumber;

        @Override
        public void process(ReplyProcessor cp) {
            cp.dataAppended(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(connectionId.getMostSignificantBits());
            out.writeLong(connectionId.getLeastSignificantBits());
            out.writeLong(eventNumber);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            UUID connectionId = new UUID(in.readLong(), in.readLong());
            long offset = in.readLong();
            return new DataAppended(connectionId, offset);
        }
    }
    
    @Data
    public static final class ConditionalCheckFailed implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.CONDITIONAL_CHECK_FAILED;
        final UUID connectionId;
        final long eventNumber;

        @Override
        public void process(ReplyProcessor cp) {
            cp.conditionalCheckFailed(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(connectionId.getMostSignificantBits());
            out.writeLong(connectionId.getLeastSignificantBits());
            out.writeLong(eventNumber);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            UUID connectionId = new UUID(in.readLong(), in.readLong());
            long offset = in.readLong();
            return new ConditionalCheckFailed(connectionId, offset);
        }
    }

    @Data
    public static final class ReadSegment implements Request, WireCommand {
        final WireCommandType type = WireCommandType.READ_SEGMENT;
        final String segment;
        final long offset;
        final int suggestedLength;

        @Override
        public void process(RequestProcessor cp) {
            cp.readSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeLong(offset);
            out.writeInt(suggestedLength);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            long offset = in.readLong();
            int suggestedLength = in.readInt();
            return new ReadSegment(segment, offset, suggestedLength);
        }
    }

    @Data
    public static final class SegmentRead implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_READ;
        final String segment;
        final long offset;
        final boolean atTail; //TODO: Is sometimes false when actual state is unknown.
        final boolean endOfSegment;
        final ByteBuffer data;

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
            int dataLength = data.remaining();
            out.writeInt(dataLength);
            out.write(data.array(), data.arrayOffset() + data.position(), data.remaining());
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            long offset = in.readLong();
            boolean atTail = in.readBoolean();
            boolean endOfSegment = in.readBoolean();
            int dataLength = in.readInt();
            if (dataLength > length) {
                throw new BufferOverflowException();
            }
            byte[] data = new byte[dataLength];
            in.readFully(data);
            return new SegmentRead(segment, offset, atTail, endOfSegment, ByteBuffer.wrap(data));
        }
    }

    @Data
    public static final class GetStreamSegmentInfo implements Request, WireCommand {
        final WireCommandType type = WireCommandType.GET_STREAM_SEGMENT_INFO;
        final String segmentName;

        @Override
        public void process(RequestProcessor cp) {
            cp.getStreamSegmentInfo(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segmentName);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            return new GetStreamSegmentInfo(segment);
        }
    }

    @Data
    public static final class StreamSegmentInfo implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.STREAM_SEGMENT_INFO;
        final String segmentName;
        final boolean exists;
        final boolean isSealed;
        final boolean isDeleted;
        final long lastModified;
        final long segmentLength;

        @Override
        public void process(ReplyProcessor cp) {
            cp.streamSegmentInfo(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segmentName);
            out.writeBoolean(exists);
            out.writeBoolean(isSealed);
            out.writeBoolean(isDeleted);
            out.writeLong(lastModified);
            out.writeLong(segmentLength);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segmentName = in.readUTF();
            boolean exists = in.readBoolean();
            boolean isSealed = in.readBoolean();
            boolean isDeleted = in.readBoolean();
            long lastModified = in.readLong();
            long segmentLength = in.readLong();
            return new StreamSegmentInfo(segmentName, exists, isSealed, isDeleted, lastModified, segmentLength);
        }
    }
    
    @Data
    public static final class GetTransactionInfo implements Request, WireCommand {
        final WireCommandType type = WireCommandType.GET_TRANSACTION_INFO;
        final String segment;
        final UUID txid;

        @Override
        public void process(RequestProcessor cp) {
            cp.getTransactionInfo(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeLong(txid.getMostSignificantBits());
            out.writeLong(txid.getLeastSignificantBits());
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            UUID txid = new UUID(in.readLong(), in.readLong());
            return new GetTransactionInfo(segment, txid);
        }
    }

    @Data
    public static final class TransactionInfo implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.TRANSACTION_INFO;
        final String segment;
        final UUID txid;
        final String transactionName;
        @Accessors(fluent = true)
        final boolean exists;
        final boolean isSealed;
        final long lastModified;
        final long dataLength;

        @Override
        public void process(ReplyProcessor cp) {
            cp.transactionInfo(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeLong(txid.getMostSignificantBits());
            out.writeLong(txid.getLeastSignificantBits());
            out.writeUTF(transactionName);
            out.writeBoolean(exists);
            out.writeBoolean(isSealed);
            out.writeLong(lastModified);
            out.writeLong(dataLength);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            UUID txid = new UUID(in.readLong(), in.readLong());
            String transactionName = in.readUTF();
            boolean exists = in.readBoolean();
            boolean isSealed = in.readBoolean();
            long lastModified = in.readLong();
            long dataLength = in.readLong();
            return new TransactionInfo(segment, txid, transactionName, exists, isSealed, lastModified, dataLength);
        }
    }

    @Data
    public static final class CreateSegment implements Request, WireCommand {
        final WireCommandType type = WireCommandType.CREATE_SEGMENT;
        final String segment;

        @Override
        public void process(RequestProcessor cp) {
            cp.createSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            return new CreateSegment(segment);
        }
    }

    @Data
    public static final class SegmentCreated implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_CREATED;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentCreated(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            return new SegmentCreated(segment);
        }
    }

    @Data
    public static final class CreateTransaction implements Request, WireCommand {
        final WireCommandType type = WireCommandType.CREATE_TRANSACTION;
        final String segment;
        final UUID txid;

        @Override
        public void process(RequestProcessor cp) {
            cp.createTransaction(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeLong(txid.getMostSignificantBits());
            out.writeLong(txid.getLeastSignificantBits());
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            UUID txid = new UUID(in.readLong(), in.readLong());
            return new CreateTransaction(segment, txid);
        }
    }

    @Data
    public static final class TransactionCreated implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.TRANSACTION_CREATED;
        final String segment;
        final UUID txid;

        @Override
        public void process(ReplyProcessor cp) {
            cp.transactionCreated(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeLong(txid.getMostSignificantBits());
            out.writeLong(txid.getLeastSignificantBits());
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            UUID txid = new UUID(in.readLong(), in.readLong());
            return new TransactionCreated(segment, txid);
        }
    }

    @Data
    public static final class CommitTransaction implements Request, WireCommand {
        final WireCommandType type = WireCommandType.COMMIT_TRANSACTION;
        final String segment;
        final UUID txid;

        @Override
        public void process(RequestProcessor cp) {
            cp.commitTransaction(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeLong(txid.getMostSignificantBits());
            out.writeLong(txid.getLeastSignificantBits());
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            UUID txid = new UUID(in.readLong(), in.readLong());
            return new CommitTransaction(segment, txid);
        }
    }

    @Data
    public static final class TransactionCommitted implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.TRANSACTION_COMMITTED;
        final String segment;
        final UUID txid;

        @Override
        public void process(ReplyProcessor cp) {
            cp.transactionCommitted(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeLong(txid.getMostSignificantBits());
            out.writeLong(txid.getLeastSignificantBits());
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            UUID txid = new UUID(in.readLong(), in.readLong());
            return new TransactionCommitted(segment, txid);
        }
    }

    @Data
    public static final class AbortTransaction implements Request, WireCommand {
        final WireCommandType type = WireCommandType.ABORT_TRANSACTION;
        final String segment;
        final UUID txid;

        @Override
        public void process(RequestProcessor cp) {
            cp.abortTransaction(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeLong(txid.getMostSignificantBits());
            out.writeLong(txid.getLeastSignificantBits());
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            UUID txid = new UUID(in.readLong(), in.readLong());
            return new AbortTransaction(segment, txid);
        }
    }

    @Data
    public static final class TransactionAborted implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.TRANSACTION_ABORTED;
        final String segment;
        final UUID txid;

        @Override
        public void process(ReplyProcessor cp) {
            cp.transactionAborted(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeLong(txid.getMostSignificantBits());
            out.writeLong(txid.getLeastSignificantBits());
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            UUID txid = new UUID(in.readLong(), in.readLong());
            return new TransactionAborted(segment, txid);
        }
    }

    
    @Data
    public static final class SealSegment implements Request, WireCommand {
        final WireCommandType type = WireCommandType.SEAL_SEGMENT;
        final String segment;

        @Override
        public void process(RequestProcessor cp) {
            cp.sealSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            return new SealSegment(segment);
        }
    }

    @Data
    public static final class SegmentSealed implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_SEALED;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentSealed(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            return new SealSegment(segment);
        }
    }

    @Data
    public static final class DeleteSegment implements Request, WireCommand {
        final WireCommandType type = WireCommandType.DELETE_SEGMENT;
        final String segment;
        
        @Override
        public void process(RequestProcessor cp) {
            cp.deleteSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            return new SealSegment(segment);
        }
    }

    @Data
    public static final class SegmentDeleted implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_DELETED;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentDeleted(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            String segment = in.readUTF();
            return new SealSegment(segment);
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
    }
    
    @Data
    public static final class Flush implements WireCommand {
        final WireCommandType type = WireCommandType.KEEP_ALIVE;
        private final int blockSize;
        
        @Override
        public void writeFields(DataOutput out) {
            throw new IllegalStateException("This command is not sent over the wire.");
        }
    }
}
