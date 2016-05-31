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
	private static final Map<Integer, Type> MAPPING;
	static {
		HashMap<Integer, Type> map = new HashMap<>();
		for (Type t : Type.values()) {
			map.put(t.getCode(), t);
		}
		MAPPING = Collections.unmodifiableMap(map);
	}

	public static Type getType(int value) {
		return MAPPING.get(value);
	}

	@FunctionalInterface
	private interface Constructor {
		WireCommand readFrom(DataInput in) throws IOException;
	}

	enum Type {
		WRONG_HOST(0, WrongHost::readFrom), SEGMENT_IS_SEALED(-1, SegmentIsSealed::readFrom), SEGMENT_ALREADY_EXISTS(-3,
				SegmentAlreadyExists::readFrom), NO_SUCH_SEGMENT(-4,
						NoSuchSegment::readFrom), NO_SUCH_BATCH(-5, NoSuchBatch::readFrom),

		SETUP_APPEND(1, SetupAppend::readFrom), APPEND_SETUP(2, AppendSetup::readFrom),

		APPEND_DATA(-100, null), // Splits into the two below for the wire.
		APPEND_DATA_HEADER(3, null), // Handled in the encoder/decoder directly
		APPEND_DATA_FOOTER(4, null), // Handled in the encoder/decoder directly
		DATA_APPENDED(5, DataAppended::readFrom),

		READ_SEGMENT(8, ReadSegment::readFrom), SEGMENT_READ(9, null), // Handled
																		// in
																		// the
																		// encoder/decoder
																		// directly

		GET_STREAM_SEGMENT_INFO(10, GetStreamSegmentInfo::readFrom), STREAM_SEGMENT_INFO(11,
				StreamSegmentInfo::readFrom),

		CREATE_SEGMENT(12, CreateSegment::readFrom), SEGMENT_CREATED(13, SegmentCreated::readFrom),

		CREATE_BATCH(14, CreateBatch::readFrom), BATCH_CREATED(15, BatchCreated::readFrom),

		MERGE_BATCH(16, MergeBatch::readFrom), BATCH_MERGED(17, BatchMerged::readFrom),

		SEAL_SEGMENT(18, SealSegment::readFrom), SEGMENT_SEALED(19, SegmentSealed::readFrom),

		DELETE_SEGMENT(20, DeleteSegment::readFrom), SEGMENT_DELETED(21, SegmentDeleted::readFrom),

		KEEP_ALIVE(100, KeepAlive::readFrom),;

		private final int code;
		private final Constructor factory;

		Type(int code, Constructor factory) {
			this.code = code;
			this.factory = factory;
		}

		public int getCode() {
			return code;
		}

		public WireCommand readFrom(DataInput in) throws IOException {
			return factory.readFrom(in);
		}
	}

	@Data
	public static final class WrongHost implements Reply {
		final WireCommands.Type type = Type.WRONG_HOST;
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
		final WireCommands.Type type = Type.SEGMENT_IS_SEALED;
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
		final WireCommands.Type type = Type.SEGMENT_ALREADY_EXISTS;
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
			return new NoSuchSegment(segment);
		}

		@Override
		public String toString() {
			return "Segment already exists: " + segment;
		}
	}

	@Data
	public static final class NoSuchSegment implements Reply {
		final WireCommands.Type type = Type.NO_SUCH_SEGMENT;
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
		final WireCommands.Type type = Type.NO_SUCH_BATCH;
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
		final WireCommands.Type type = Type.SETUP_APPEND;
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
		final WireCommands.Type type = Type.APPEND_SETUP;
		final String segment;
		final UUID connectionId;
		final long connectionOffsetAckLevel;

		@Override
		public void process(ReplyProcessor cp) {
			cp.appendSetup(this);
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
	public static final class AppendData implements Request, Comparable<AppendData> {
		final WireCommands.Type type = Type.APPEND_DATA;
		final String segment;
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
		final WireCommands.Type type = Type.DATA_APPENDED;
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
		final WireCommands.Type type = Type.READ_SEGMENT;
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
		final WireCommands.Type type = Type.SEGMENT_READ;
		final String segment;
		final long offset;
		final ByteBuffer data;
		final boolean atTail;
		final boolean endOfStream;

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
		final WireCommands.Type type = Type.GET_STREAM_SEGMENT_INFO;
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
		final WireCommands.Type type = Type.STREAM_SEGMENT_INFO;
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
		final WireCommands.Type type = Type.CREATE_SEGMENT;
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
		final WireCommands.Type type = Type.SEGMENT_CREATED;

		@Override
		public void process(ReplyProcessor cp) {
			cp.segmentCreated(this);
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
	public static final class CreateBatch implements Request {
		final WireCommands.Type type = Type.CREATE_BATCH;

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
		final WireCommands.Type type = Type.BATCH_CREATED;

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
		final WireCommands.Type type = Type.MERGE_BATCH;

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
		final WireCommands.Type type = Type.BATCH_MERGED;

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
		final WireCommands.Type type = Type.SEAL_SEGMENT;

		@Override
		public void process(RequestProcessor cp) {
			cp.sealSegment(this);
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
	public static final class SegmentSealed implements Reply {
		final WireCommands.Type type = Type.SEGMENT_SEALED;

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
		final WireCommands.Type type = Type.DELETE_SEGMENT;

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
		final WireCommands.Type type = Type.SEGMENT_DELETED;

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
		final WireCommands.Type type = Type.KEEP_ALIVE;

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
			// TODO Auto-generated method stub

		}

		public static WireCommand readFrom(DataInput in) {
			return null;
		}
	}
}
