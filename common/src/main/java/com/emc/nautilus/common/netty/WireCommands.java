package com.emc.nautilus.common.netty;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
		WRONG_HOST(0, WrongHost::readFrom),
		SEGMENT_IS_SEALED(-1, SegmentIsSealed::readFrom),
		END_OF_STREAM(-2, EndOfStream::readFrom),
		NO_SUCH_STREAM(-3, NoSuchStream::readFrom),
		NO_SUCH_SEGMENT(-4, NoSuchSegment::readFrom),
		NO_SUCH_BATCH(-5, NoSuchBatch::readFrom),

		SETUP_APPEND(1, SetupAppend::readFrom),
		APPEND_SETUP(2, AppendSetup::readFrom),

		APPEND_DATA(-100, null),//Splits into the two below for the wire.
		APPEND_DATA_HEADER(3, null),
		APPEND_DATA_FOOTER(4, null),
		DATA_APPENDED(5, DataAppended::readFrom),

		SETUP_READ(6, SetupRead::readFrom),
		READ_SETUP(7, ReadSetup::readFrom),

		READ_SEGMENT(8, ReadSegment::readFrom),
		SEGMENT_READ(9, SegmentRead::readFrom),

		GET_STREAM_INFO(10, GetStreamInfo::readFrom),
		STREAM_INFO(11, StreamInfo::readFrom),

		CREATE_STREAMS_SEGMENT(12, CreateStreamsSegment::readFrom),
		STREAMS_SEGMENT_CREATED(13, StreamsSegmentCreated::readFrom),

		CREATE_BATCH(14, CreateBatch::readFrom),
		BATCH_CREATED(15, BatchCreated::readFrom),

		MERGE_BATCH(16, MergeBatch::readFrom),
		BATCH_MERGED(17, BatchMerged::readFrom),

		SEAL_SEGMENT(18, SealSegment::readFrom),
		SEGMENT_SEALED(19, SegmentSealed::readFrom),

		DELETE_SEGMENT(20, DeleteSegment::readFrom),
		SEGMENT_DELETED(21, SegmentDeleted::readFrom),

		KEEP_ALIVE(100, KeepAlive::readFrom),
		;

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
	public static final class WrongHost implements WireCommand {
		final WireCommands.Type type = Type.WRONG_HOST;

		@Override
		public void process(CommandProcessor cp) {
			cp.wrongHost(this);
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
	public static final class SegmentIsSealed implements WireCommand {
		final WireCommands.Type type = Type.SEGMENT_IS_SEALED;

		@Override
		public void process(CommandProcessor cp) {
			cp.segmentIsSealed(this);
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
	public static final class EndOfStream implements WireCommand {
		final WireCommands.Type type = Type.END_OF_STREAM;

		@Override
		public void process(CommandProcessor cp) {
			cp.endOfStream(this);
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
	public static final class NoSuchStream implements WireCommand {
		final WireCommands.Type type = Type.NO_SUCH_STREAM;

		@Override
		public void process(CommandProcessor cp) {
			cp.noSuchStream(this);
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
	public static final class NoSuchSegment implements WireCommand {
		final WireCommands.Type type = Type.NO_SUCH_SEGMENT;

		@Override
		public void process(CommandProcessor cp) {
			cp.noSuchSegment(this);
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
	public static final class NoSuchBatch implements WireCommand {
		final WireCommands.Type type = Type.NO_SUCH_BATCH;

		@Override
		public void process(CommandProcessor cp) {
			cp.noSuchBatch(this);
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
	public static final class SetupAppend implements WireCommand {
		final WireCommands.Type type = Type.SETUP_APPEND;
		final String segment;

		@Override
		public void process(CommandProcessor cp) {
			cp.setupAppend(this);
		}

		@Override
		public void writeFields(DataOutput out) throws IOException {
			out.writeUTF(segment);
		}

		public static WireCommand readFrom(DataInput in) throws IOException {
			String segment = in.readUTF();
			return new SetupAppend(segment);
		}
	}

	@Data
	public static final class AppendSetup implements WireCommand {
		final WireCommands.Type type = Type.APPEND_SETUP;
		final String segment;

		@Override
		public void process(CommandProcessor cp) {
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
	public static final class AppendData implements WireCommand {
		final WireCommands.Type type = Type.APPEND_DATA;
		final String segment;
		final long connectionOffset;
		final ByteBuf data;

		@Override
		public void process(CommandProcessor cp) {
			cp.appendData(this);
		}

		@Override
		public void writeFields(DataOutput out) {
			throw new UnsupportedOperationException();
		}
	}

	@Data
	public static final class DataAppended implements WireCommand {
		final WireCommands.Type type = Type.DATA_APPENDED;
		final String segment;
		final long connectionOffset;

		@Override
		public void process(CommandProcessor cp) {
			cp.dataAppended(this);
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
	public static final class SetupRead implements WireCommand {
		final WireCommands.Type type = Type.SETUP_READ;

		@Override
		public void process(CommandProcessor cp) {
			cp.setupRead(this);
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
	public static final class ReadSetup implements WireCommand {
		final WireCommands.Type type = Type.READ_SETUP;

		@Override
		public void process(CommandProcessor cp) {
			cp.readSetup(this);
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
	public static final class ReadSegment implements WireCommand {
		final WireCommands.Type type = Type.READ_SEGMENT;

		@Override
		public void process(CommandProcessor cp) {
			cp.readSegment(this);
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
	public static final class SegmentRead implements WireCommand {
		final WireCommands.Type type = Type.SEGMENT_READ;

		@Override
		public void process(CommandProcessor cp) {
			cp.segmentRead(this);
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
	public static final class GetStreamInfo implements WireCommand {
		final WireCommands.Type type = Type.GET_STREAM_INFO;

		@Override
		public void process(CommandProcessor cp) {
			cp.getStreamInfo(this);
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
	public static final class StreamInfo implements WireCommand {
		final WireCommands.Type type = Type.STREAM_INFO;

		@Override
		public void process(CommandProcessor cp) {
			cp.streamInfo(this);
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
	public static final class CreateStreamsSegment implements WireCommand {
		final WireCommands.Type type = Type.CREATE_STREAMS_SEGMENT;

		@Override
		public void process(CommandProcessor cp) {
			cp.createStreamsSegment(this);
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
	public static final class StreamsSegmentCreated implements WireCommand {
		final WireCommands.Type type = Type.STREAMS_SEGMENT_CREATED;

		@Override
		public void process(CommandProcessor cp) {
			cp.streamsSegmentCreated(this);
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
	public static final class CreateBatch implements WireCommand {
		final WireCommands.Type type = Type.CREATE_BATCH;

		@Override
		public void process(CommandProcessor cp) {
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
	public static final class BatchCreated implements WireCommand {
		final WireCommands.Type type = Type.BATCH_CREATED;

		@Override
		public void process(CommandProcessor cp) {
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
	public static final class MergeBatch implements WireCommand {
		final WireCommands.Type type = Type.MERGE_BATCH;

		@Override
		public void process(CommandProcessor cp) {
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
	public static final class BatchMerged implements WireCommand {
		final WireCommands.Type type = Type.BATCH_MERGED;

		@Override
		public void process(CommandProcessor cp) {
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
	public static final class SealSegment implements WireCommand {
		final WireCommands.Type type = Type.SEAL_SEGMENT;

		@Override
		public void process(CommandProcessor cp) {
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
	public static final class SegmentSealed implements WireCommand {
		final WireCommands.Type type = Type.SEGMENT_SEALED;

		@Override
		public void process(CommandProcessor cp) {
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
	public static final class DeleteSegment implements WireCommand {
		final WireCommands.Type type = Type.DELETE_SEGMENT;

		@Override
		public void process(CommandProcessor cp) {
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
	public static final class SegmentDeleted implements WireCommand {
		final WireCommands.Type type = Type.SEGMENT_DELETED;

		@Override
		public void process(CommandProcessor cp) {
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
	public static final class KeepAlive implements WireCommand {
		final WireCommands.Type type = Type.KEEP_ALIVE;

		@Override
		public void process(CommandProcessor cp) {
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
