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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import lombok.Data;

public final class WireCommands {

    public static final int APPEND_BLOCK_SIZE = 32 * 1024;
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

    @FunctionalInterface interface Constructor {
        WireCommand readFrom(DataInput in) throws IOException;
    }

    @Data
    public static final class WrongHost implements Reply {
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

        public static WireCommand readFrom(DataInput in) throws IOException {
            String segment = in.readUTF();
            String correctHost = in.readUTF();
            return new WrongHost(segment, correctHost);
        }
    }

    @Data
    public static final class SegmentIsSealed implements Reply {
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

        public static WireCommand readFrom(DataInput in) throws IOException {
            String segment = in.readUTF();
            return new SegmentIsSealed(segment);
        }
    }

    @Data
    public static final class SegmentAlreadyExists implements Reply {
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

        public static WireCommand readFrom(DataInput in) throws IOException {
            String segment = in.readUTF();
            return new SegmentAlreadyExists(segment);
        }

        @Override
        public String toString() {
            return "Segment already exists: " + segment;
        }
    }

    @Data
    public static final class NoSuchSegment implements Reply {
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

        public static WireCommand readFrom(DataInput in) throws IOException {
            String segment = in.readUTF();
            return new NoSuchSegment(segment);
        }

        @Override
        public String toString() {
            return "No such segment: " + segment;
        }
    }

    @Data
    public static final class NoSuchBatch implements Reply {
        final WireCommandType type = WireCommandType.NO_SUCH_BATCH;
        final String batch;

        @Override
        public void process(ReplyProcessor cp) {
            cp.noSuchBatch(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(batch);
        }

        public static WireCommand readFrom(DataInput in) throws IOException {
            String batch = in.readUTF();
            return new NoSuchBatch(batch);
        }

        @Override
        public String toString() {
            return "No such batch: " + batch;
        }
    }

    @Data
    public static final class SetupAppend implements Request {
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

        public static WireCommand readFrom(DataInput in) throws IOException {
            UUID uuid = new UUID(in.readLong(), in.readLong());
            String segment = in.readUTF();
            return new SetupAppend(uuid, segment);
        }
    }

    @Data
    public static final class AppendSetup implements Reply {
        final WireCommandType type = WireCommandType.APPEND_SETUP;
        final String segment;
        final UUID connectionId;
        final long connectionOffsetAckLevel;

        @Override
        public void process(ReplyProcessor cp) {
            cp.appendSetup(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeLong(connectionId.getMostSignificantBits());
            out.writeLong(connectionId.getLeastSignificantBits());
            out.writeLong(connectionOffsetAckLevel);
        }

        public static WireCommand readFrom(DataInput in) throws IOException {
            String segment = in.readUTF();
            UUID connectionId = new UUID(in.readLong(), in.readLong());
            long connectionOffsetAckLevel = in.readLong();
            return new AppendSetup(segment, connectionId, connectionOffsetAckLevel);
        }
    }

    @Data
    public static final class AppendData implements Request, Comparable<AppendData> {
        final WireCommandType type = WireCommandType.APPEND_DATA;
        final UUID connectionId;
        final long connectionOffset;
        final ByteBuf data;

        @Override
        public void process(RequestProcessor cp) {
            cp.appendData(this);
        }

        @Override
        public void writeFields(DataOutput out) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(AppendData other) {
            return Long.compare(connectionOffset, other.connectionOffset);
        }
    }

    @Data
    public static final class DataAppended implements Reply {
        final WireCommandType type = WireCommandType.DATA_APPENDED;
        final String segment;
        final long connectionOffset;

        @Override
        public void process(ReplyProcessor cp) {
            cp.dataAppended(this);
        }

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeUTF(segment);
            out.writeLong(connectionOffset);
        }

        public static WireCommand readFrom(DataInput in) throws IOException {
            String segment = in.readUTF();
            long offset = in.readLong();
            return new DataAppended(segment, offset);
        }
    }

    @Data
    public static final class ReadSegment implements Request {
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

        public static WireCommand readFrom(DataInput in) throws IOException {
            String segment = in.readUTF();
            long offset = in.readLong();
            int suggestedLength = in.readInt();
            return new ReadSegment(segment, offset, suggestedLength);
        }
    }

    @Data
    public static final class SegmentRead implements Reply {
        final WireCommandType type = WireCommandType.SEGMENT_READ;
        final String segment;
        final long offset;
        final boolean atTail;
        final boolean endOfStream;
        final ByteBuffer data;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentRead(this);
        }

        @Override
        public void writeFields(DataOutput out) {
            throw new UnsupportedOperationException();
        }
    }

    @Data
    public static final class GetStreamSegmentInfo implements Request {
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

        public static WireCommand readFrom(DataInput in) throws IOException {
            String segment = in.readUTF();
            return new GetStreamSegmentInfo(segment);
        }
    }

    @Data
    public static final class StreamSegmentInfo implements Reply {
        final WireCommandType type = WireCommandType.STREAM_SEGMENT_INFO;
        final String segmentName;
        final boolean exists;
        final boolean isSealed;
        final boolean isDeleted;
        final long lastModified;
        final long length;

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
            out.writeLong(length);
        }

        public static WireCommand readFrom(DataInput in) throws IOException {
            String segmentName = in.readUTF();
            boolean exists = in.readBoolean();
            boolean isSealed = in.readBoolean();
            boolean isDeleted = in.readBoolean();
            long lastModified = in.readLong();
            long length = in.readLong();
            return new StreamSegmentInfo(segmentName, exists, isSealed, isDeleted, lastModified, length);
        }
    }

    @Data
    public static final class CreateSegment implements Request {
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

        public static WireCommand readFrom(DataInput in) throws IOException {
            String segment = in.readUTF();
            return new CreateSegment(segment);
        }
    }

    @Data
    public static final class SegmentCreated implements Reply {
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

        public static WireCommand readFrom(DataInput in) throws IOException {
            String segment = in.readUTF();
            return new SegmentCreated(segment);
        }
    }

    @Data
    public static final class CreateBatch implements Request {
        final WireCommandType type = WireCommandType.CREATE_BATCH;

        @Override
        public void process(RequestProcessor cp) {
            cp.createBatch(this);
        }

        @Override
        public void writeFields(DataOutput out) {
            // TODO Auto-generated method stub

        }

        public static WireCommand readFrom(DataInput in) {
            return null;
        }
    }

    @Data
    public static final class BatchCreated implements Reply {
        final WireCommandType type = WireCommandType.BATCH_CREATED;

        @Override
        public void process(ReplyProcessor cp) {
            cp.batchCreated(this);
        }

        @Override
        public void writeFields(DataOutput out) {
            // TODO Auto-generated method stub

        }

        public static WireCommand readFrom(DataInput in) {
            return null;
        }
    }

    @Data
    public static final class MergeBatch implements Request {
        final WireCommandType type = WireCommandType.MERGE_BATCH;

        @Override
        public void process(RequestProcessor cp) {
            cp.mergeBatch(this);
        }

        @Override
        public void writeFields(DataOutput out) {
            // TODO Auto-generated method stub

        }

        public static WireCommand readFrom(DataInput in) {
            return null;
        }
    }

    @Data
    public static final class BatchMerged implements Reply {
        final WireCommandType type = WireCommandType.BATCH_MERGED;

        @Override
        public void process(ReplyProcessor cp) {
            cp.batchMerged(this);
        }

        @Override
        public void writeFields(DataOutput out) {
            // TODO Auto-generated method stub

        }

        public static WireCommand readFrom(DataInput in) {
            return null;
        }
    }

    @Data
    public static final class SealSegment implements Request {
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

        public static WireCommand readFrom(DataInput in) throws IOException {
            String segment = in.readUTF();
            return new SealSegment(segment);
        }
    }

    @Data
    public static final class SegmentSealed implements Reply {
        final WireCommandType type = WireCommandType.SEGMENT_SEALED;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentSealed(this);
        }

        @Override
        public void writeFields(DataOutput out) {
            // TODO Auto-generated method stub

        }

        public static WireCommand readFrom(DataInput in) {
            return null;
        }
    }

    @Data
    public static final class DeleteSegment implements Request {
        final WireCommandType type = WireCommandType.DELETE_SEGMENT;

        @Override
        public void process(RequestProcessor cp) {
            cp.deleteSegment(this);
        }

        @Override
        public void writeFields(DataOutput out) {
            // TODO Auto-generated method stub

        }

        public static WireCommand readFrom(DataInput in) {
            return null;
        }
    }

    @Data
    public static final class SegmentDeleted implements Reply {
        final WireCommandType type = WireCommandType.SEGMENT_DELETED;

        @Override
        public void process(ReplyProcessor cp) {
            cp.segmentDeleted(this);
        }

        @Override
        public void writeFields(DataOutput out) {
            // TODO Auto-generated method stub

        }

        public static WireCommand readFrom(DataInput in) {
            return null;
        }
    }

    @Data
    public static final class KeepAlive implements Request, Reply {
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

        public static WireCommand readFrom(DataInput in) {
            return new KeepAlive();
        }
    }
}
