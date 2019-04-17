/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

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
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import lombok.Data;
import lombok.experimental.Accessors;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static java.nio.charset.StandardCharsets.UTF_8;

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
    public static final int WIRE_VERSION = 8;
    public static final int OLDEST_COMPATIBLE_VERSION = 5;
    public static final int TYPE_SIZE = 4;
    public static final int TYPE_PLUS_LENGTH_SIZE = 8;
    public static final int MAX_WIRECOMMAND_SIZE = 0x007FFFFF; // 8MB
    
    public static final long NULL_ATTRIBUTE_VALUE = Long.MIN_VALUE; //This is the same as Attributes.NULL_ATTRIBUTE_VALUE
    
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
        WireCommand readFrom(ByteBufInputStream in, int length) throws IOException;
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
    public static final class InvalidEventNumber implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.INVALID_EVENT_NUMBER;
        final UUID writerId;
        final long eventNumber;
        final String serverStackTrace;

        @Override
        public void process(ReplyProcessor cp) {
            cp.invalidEventNumber(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeLong(eventNumber);
            out.writeUTF(serverStackTrace);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            UUID writerId = new UUID(in.readLong(), in.readLong());
            long eventNumber = in.readLong();
            String serverStackTrace = (in.available() > 0) ? in.readUTF() : EMPTY_STACK_TRACE;
            return new InvalidEventNumber(writerId, eventNumber, serverStackTrace);
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
            return eventNumber;
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
    public static final class PartialEvent implements WireCommand {
        final WireCommandType type = WireCommandType.PARTIAL_EVENT;
        final ByteBuf data;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            data.getBytes(data.readerIndex(), (OutputStream) out, data.readableBytes());
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
            out.writeInt(type.getCode());
            out.writeInt(data.readableBytes());
            data.getBytes(data.readerIndex(), (OutputStream) out, data.readableBytes());
        }

        public ByteBuf getAsByteBuf() {
            ByteBuf header = Unpooled.buffer(TYPE_PLUS_LENGTH_SIZE, TYPE_PLUS_LENGTH_SIZE);
            header.writeInt(type.getCode());
            header.writeInt(data.readableBytes());
            return Unpooled.wrappedBuffer(header, data);
        }
    }

    @Data
    public static final class SetupAppend implements Request, WireCommand {
        final WireCommandType type = WireCommandType.SETUP_APPEND;
        final long requestId;
        final UUID writerId;
        final String segment;
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
    public static final class AppendBlock implements WireCommand {
        final WireCommandType type = WireCommandType.APPEND_BLOCK;
        final UUID writerId;
        final ByteBuf data;
        final long requestId;

        AppendBlock(long requestId, UUID writerId) {
            this.writerId = writerId;
            this.data = Unpooled.EMPTY_BUFFER; // Populated on read path
            this.requestId = requestId;
        }

        AppendBlock(long requestId, UUID writerId, ByteBuf data) {
            this.writerId = writerId;
            this.data = data;
            this.requestId = requestId;
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeLong(requestId);
            // Data not written, as it should be null.
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            UUID writerId = new UUID(in.readLong(), in.readLong());
            byte[] data = new byte[length - Long.BYTES * 3];
            long requestId = (in.available() >= Long.BYTES) ? in.readLong() : -1L;
            in.readFully(data);
            return new AppendBlock(requestId, writerId, wrappedBuffer(data));
        }
    }

    @Data
    public static final class AppendBlockEnd implements WireCommand {
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

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            UUID writerId = new UUID(in.readLong(), in.readLong());
            int sizeOfHeaderlessAppends = in.readInt();
            int dataLength = in.readInt();
            byte[] data;
            if (dataLength > 0) {
                data = new byte[dataLength];
                in.readFully(data);
            } else {
                data = new byte[0];
            }
            int numEvents = in.readInt();
            long lastEventNumber = in.readLong();
            long requestId = in.available() >= Long.BYTES ? in.readLong() : -1L;
            return new AppendBlockEnd(writerId, sizeOfHeaderlessAppends, wrappedBuffer(data), numEvents, lastEventNumber, requestId);
        }
    }

    @Data
    public static final class ConditionalAppend implements WireCommand, Request {
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

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            UUID writerId = new UUID(in.readLong(), in.readLong());
            long eventNumber = in.readLong();
            long expectedOffset = in.readLong();
            Event event = readEvent(in, length);
            long requestId = (in.available() >= Long.BYTES) ? in.readLong() : -1L;
            return new ConditionalAppend(writerId, eventNumber, expectedOffset, event, requestId);
        }

        private static Event readEvent(ByteBufInputStream in, int length) throws IOException {
            int typeCode = in.readInt();
            if (typeCode != WireCommandType.EVENT.getCode()) {
                throw new InvalidMessageException("Was expecting EVENT but found: " + typeCode);
            }
            int eventLength = in.readInt();
            if (eventLength > length - TYPE_PLUS_LENGTH_SIZE) {
                throw new InvalidMessageException("Was expecting length: " + length + " but found: " + eventLength);
            }
            byte[] msg = new byte[eventLength];
            in.readFully(msg);
            return new Event(wrappedBuffer(msg));
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
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            UUID writerId = new UUID(in.readLong(), in.readLong());
            long offset = in.readLong();
            long previousEventNumber = -1;
            if (length >= 32) {
                previousEventNumber = in.readLong();
            }
            long requestId = in.available() >= Long.BYTES ? in.readLong() : -1L;

            return new DataAppended(requestId, writerId, offset, previousEventNumber);
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
    public static final class ReadSegment implements Request, WireCommand {
        final WireCommandType type = WireCommandType.READ_SEGMENT;
        final String segment;
        final long offset;
        final int suggestedLength;
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

    @Data
    public static final class SegmentRead implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_READ;
        final String segment;
        final long offset;
        final boolean atTail; //TODO: Is sometimes false when actual state is unknown.
        final boolean endOfSegment;
        final ByteBuffer data;
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
            int dataLength = data.remaining();
            out.writeInt(dataLength);
            out.write(data.array(), data.arrayOffset() + data.position(), data.remaining());
            out.writeLong(requestId);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
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
            long requestId =  in.available() >= Long.BYTES ? in.readLong() : -1L;
            return new SegmentRead(segment, offset, atTail, endOfSegment, ByteBuffer.wrap(data), requestId);
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
            return new StreamSegmentInfo(requestId, segmentName, exists, isSealed, isDeleted, lastModified, segmentLength, startOffset);
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
        final String delegationToken;

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
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            int desiredRate = in.readInt();
            byte scaleType = in.readByte();
            String delegationToken = in.readUTF();

            return new CreateSegment(requestId, segment, scaleType, desiredRate, delegationToken);
        }
    }

    @Data
    public static final class CreateTableSegment implements Request, WireCommand {

        final WireCommandType type = WireCommandType.CREATE_TABLE_SEGMENT;
        final long requestId;
        final String segment;
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.createTableSegment(this);
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

            return new CreateTableSegment(requestId, segment, delegationToken);
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
    public static final class UpdateSegmentPolicy implements Request, WireCommand {

        final WireCommandType type = WireCommandType.UPDATE_SEGMENT_POLICY;
        final long requestId;
        final String segment;
        final byte scaleType;
        final int targetRate;
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
    public static final class MergeSegments implements Request, WireCommand {
        final WireCommandType type = WireCommandType.MERGE_SEGMENTS;
        final long requestId;
        final String target;
        final String source;
        final String delegationToken;

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
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String target = in.readUTF();
            String source = in.readUTF();
            String delegationToken = in.readUTF();
            return new MergeSegments(requestId, target, source, delegationToken);
        }
    }

    @Data
    public static final class MergeTableSegments implements Request, WireCommand {
        final WireCommandType type = WireCommandType.MERGE_TABLE_SEGMENTS;
        final long requestId;
        final String target;
        final String source;
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.mergeTableSegments(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(target);
            out.writeUTF(source);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String target = in.readUTF();
            String source = in.readUTF();
            String delegationToken = in.readUTF();
            return new MergeTableSegments(requestId, target, source, delegationToken);
        }
    }

    @Data
    public static final class SegmentsMerged implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENTS_MERGED;
        final long requestId;
        final String target;
        final String source;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentsMerged(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(target);
            out.writeUTF(source);
        }

        public static WireCommand readFrom(DataInput in, int length) throws IOException {
            long requestId = in.readLong();
            String target = in.readUTF();
            String source = in.readUTF();
            return new SegmentsMerged(requestId, target, source);
        }
    }

    @Data
    public static final class SealSegment implements Request, WireCommand {
        final WireCommandType type = WireCommandType.SEAL_SEGMENT;
        final long requestId;
        final String segment;
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
    public static final class SealTableSegment implements Request, WireCommand {
        final WireCommandType type = WireCommandType.SEAL_TABLE_SEGMENT;
        final long requestId;
        final String segment;
        final String delegationToken;

        @Override
        public void process(RequestProcessor cp) {
            cp.sealTableSegment(this);
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
            return new SealTableSegment(requestId, segment, delegationToken);
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
    }

    @Data
    public static final class AuthTokenCheckFailed implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.AUTH_TOKEN_CHECK_FAILED;
        final long requestId;
        final String serverStackTrace;

        @Override
        public void process(ReplyProcessor cp) {
            cp.authTokenCheckFailed(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(serverStackTrace);
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String serverStackTrace = (in.available() > 0) ? in.readUTF() : EMPTY_STACK_TRACE;
            return new AuthTokenCheckFailed(requestId, serverStackTrace);
        }
    }

    @Data
    public static final class UpdateTableEntries implements Request, WireCommand {

        final WireCommandType type = WireCommandType.UPDATE_TABLE_ENTRIES;
        final long requestId;
        final String segment;
        final String delegationToken;
        final TableEntries tableEntries;

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
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            TableEntries entries = (TableEntries) TableEntries.readFrom(in, in.available());

            return new UpdateTableEntries(requestId, segment, delegationToken, entries);
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
    public static final class RemoveTableKeys implements Request, WireCommand {

        final WireCommandType type = WireCommandType.REMOVE_TABLE_KEYS;
        final long requestId;
        final String segment;
        final String delegationToken;
        final List<TableKey> keys;

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
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            int numberOfKeys = in.readInt();
            List<TableKey> keys = new ArrayList<>(numberOfKeys);
            for (int i = 0; i < numberOfKeys; i++) {
                keys.add((TableKey) TableKey.readFrom(in, in.available()));
            }
            return new RemoveTableKeys(requestId, segment, delegationToken, keys);
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
    public static final class ReadTable implements Request, WireCommand {

        final WireCommandType type = WireCommandType.READ_TABLE;
        final long requestId;
        final String segment;
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

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            int numberOfKeys = in.readInt();
            List<TableKey> keys = new ArrayList<>(numberOfKeys);
            for (int i = 0; i < numberOfKeys; i++) {
                keys.add((TableKey) TableKey.readFrom(in, in.available()));
            }
            return new ReadTable(requestId, segment, delegationToken, keys);
        }
    }

    @Data
    public static final class TableRead implements Reply, WireCommand {
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

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            TableEntries entries = TableEntries.readFrom(in, in.available());
            return new TableRead(requestId, segment, entries);
        }
    }

    @Data
    public static final class ReadTableKeys implements Request, WireCommand {

        final WireCommandType type = WireCommandType.READ_TABLE_KEYS;
        final long requestId;
        final String segment;
        final String delegationToken;
        final int suggestedKeyCount;
        final ByteBuf continuationToken; // this is used to indicate the point from which the next keys should be fetched.

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
            out.writeInt(continuationToken.readableBytes()); // continuation token length.
            if (continuationToken.readableBytes() != 0) {
                continuationToken.getBytes(continuationToken.readerIndex(), (OutputStream) out, continuationToken.readableBytes());
            }
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            int suggestedKeyCount = in.readInt();
            int dataLength = in.readInt();

            if (length < dataLength + Long.BYTES + segment.getBytes(UTF_8).length + delegationToken.getBytes(UTF_8).length + 2 * Integer.BYTES) {
                throw new InvalidMessageException("Was expecting length: " + length + " but found: " + dataLength);
            }
            byte[] continuationToken = new byte[dataLength];
            in.readFully(continuationToken);

            return new ReadTableKeys(requestId, segment, delegationToken, suggestedKeyCount, wrappedBuffer(continuationToken));
        }
    }

    @Data
    public static final class TableKeysRead implements Reply, WireCommand {
        public static final Function<Integer, Integer> GET_HEADER_BYTES =
                keyCount -> Long.BYTES + Integer.BYTES + TableKey.HEADER_BYTES * keyCount + Integer.BYTES;

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

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            int numberOfKeys = in.readInt();
            List<TableKey> keys = new ArrayList<>(numberOfKeys);
            int keyByteCount = 0;
            for (int i = 0; i < numberOfKeys; i++) {
                TableKey k = TableKey.readFrom(in, in.available());
                keys.add(k);
                keyByteCount += TableKey.HEADER_BYTES + Long.BYTES + k.getData().readableBytes();
            }
            int dataLength = in.readInt();
            if (length < dataLength + Long.BYTES + segment.getBytes(UTF_8).length + Integer.BYTES + keyByteCount) {
                throw new InvalidMessageException("Was expecting length: " + length + " but found: " + dataLength);
            }
            byte[] continuationToken = new byte[dataLength];
            in.readFully(continuationToken);

            return new TableKeysRead(requestId, segment, keys, wrappedBuffer(continuationToken));
        }
    }

    @Data
    public static final class ReadTableEntries implements Request, WireCommand {

        final WireCommandType type = WireCommandType.READ_TABLE_ENTRIES;
        final long requestId;
        final String segment;
        final String delegationToken;
        final int suggestedEntryCount;
        final ByteBuf continuationToken; // this is used to indicate the point from which the next entry should be fetched.

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
            out.writeInt(continuationToken.readableBytes()); // continuation token length.
            if (continuationToken.readableBytes() != 0) {
                continuationToken.getBytes(continuationToken.readerIndex(), (OutputStream) out, continuationToken.readableBytes());
            }
        }

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            String delegationToken = in.readUTF();
            int suggestedEntryCount = in.readInt();
            int dataLength = in.readInt();

            if (length < dataLength + Long.BYTES + segment.getBytes(UTF_8).length + delegationToken.getBytes(UTF_8).length + 2 * Integer.BYTES ) {
                throw new InvalidMessageException("Was expecting length: " + length + " but found: " + dataLength);
            }

            byte[] continuationToken = new byte[dataLength];
            in.readFully(continuationToken);

            return new ReadTableEntries(requestId, segment, delegationToken, suggestedEntryCount, wrappedBuffer(continuationToken));
        }
    }

    @Data
    public static final class TableEntriesRead implements Reply, WireCommand {
        public static final Function<Integer, Integer> GET_HEADER_BYTES =
                entryCount -> Long.BYTES + TableEntries.GET_HEADER_BYTES.apply(entryCount) + Integer.BYTES;

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

        public static WireCommand readFrom(ByteBufInputStream in, int length) throws IOException {
            long requestId = in.readLong();
            String segment = in.readUTF();
            TableEntries entries = TableEntries.readFrom(in, in.available());
            int dataLength = in.readInt();

            if (length < dataLength + Long.BYTES + segment.getBytes(UTF_8).length + entries.size() + Integer.BYTES ) {
                throw new InvalidMessageException("Was expecting length: " + length + " but found: " + dataLength);
            }

            byte[] continuationToken = new byte[dataLength];
            in.readFully(continuationToken);

            return new TableEntriesRead(requestId, segment, entries, wrappedBuffer(continuationToken));
        }
    }

    @Data
    public static final class TableEntries {
        static final Function<Integer, Integer> GET_HEADER_BYTES =
                entryCount -> Integer.BYTES + entryCount * (TableKey.HEADER_BYTES + TableValue.HEADER_BYTES);

        final List<Map.Entry<TableKey, TableValue>> entries;

        public void writeFields(DataOutput out) throws IOException {
            out.writeInt(entries.size());
            for (Map.Entry<TableKey, TableValue> ent : entries) {
                ent.getKey().writeFields(out);
                ent.getValue().writeFields(out);
            }
        }

        public static TableEntries readFrom(ByteBufInputStream in, int length) throws IOException {
            int numberOfEntries = in.readInt();
            List<Map.Entry<TableKey, TableValue>> entries = new ArrayList<>();
            for (int i = 0; i < numberOfEntries; i++) {
                entries.add(new AbstractMap.SimpleImmutableEntry<>(TableKey.readFrom(in, in.available()),
                                                                   TableValue.readFrom(in, in.available())));
            }

            return new TableEntries(entries);
        }

        public int size() {
            int dataBytes = entries.stream()
                                   .mapToInt(e -> e.getKey().getData().readableBytes() + Long.BYTES + e.getValue().getData().readableBytes())
                                   .sum();
            return GET_HEADER_BYTES.apply(entries.size()) + dataBytes;
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

        public static TableKey readFrom(DataInput in, int length) throws IOException {
            int payLoadSize = in.readInt();
            int dataLength = in.readInt();
            if (dataLength == 0) {
                return TableKey.EMPTY;
            }
            if (length < payLoadSize) {
                throw new InvalidMessageException("Was expecting length of at least : " + payLoadSize + " but found: " + length);
            }
            byte[] msg = new byte[dataLength];
            in.readFully(msg);
            long keyVersion = in.readLong();
            return new TableKey(wrappedBuffer(msg), keyVersion);
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

        public static TableValue readFrom(DataInput in, int length) throws IOException {
            int payloadSize = in.readInt();
            int valueLength = in.readInt();
            if (valueLength == 0) {
                return TableValue.EMPTY;
            }
            if ( length < payloadSize) {
                throw new InvalidMessageException("Was expecting length of at least : " + payloadSize + " but found: " + length);
            }
            byte[] msg = new byte[valueLength];
            in.readFully(msg);
            return new TableValue(wrappedBuffer(msg));
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

}
