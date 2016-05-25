package com.emc.nautilus.common.netty;

import java.util.List;

import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.common.netty.WireCommands.Type;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.Cleanup;

public class CommandDecoder extends ByteToMessageDecoder {

	private String appendingSegment;
	private long connectionOffset;
	private ByteBuf appendHeader;

	public CommandDecoder() {
		appendingSegment = null;
		connectionOffset = 0;
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
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
			String readSegment = is.readUTF();
			checkSegment(readSegment);
			checkOffset(connectionOffset, readOffset);
			if (appendHeader != null) {
				throw new IllegalStateException("Header appended immediatly after header.");
			}
			appendHeader = in.readBytes(length - (in.readerIndex() - dataStart));
			break;
		}
		case APPEND_DATA_FOOTER: {
			long readOffset = is.readLong();
			String readSegment = is.readUTF();
			int footerDataLength = length - (in.readerIndex() - dataStart);
			if (appendHeader == null) {
				throw new IllegalStateException("Footer not following header.");
			}
			int appendLength = appendHeader.readableBytes() + footerDataLength;
			checkSegment(readSegment);
			checkOffset(connectionOffset + appendLength, readOffset);
			ByteBuf footerData = in.readBytes(footerDataLength);
			out.add(new AppendData(appendingSegment,
					connectionOffset,
					Unpooled.wrappedBuffer(appendHeader, footerData)));
			connectionOffset += appendLength;
			appendHeader = null;
			break;
		}
		case SETUP_APPEND: {
			SetupAppend setupAppend = (SetupAppend) type.readFrom(is);
			appendingSegment = setupAppend.getSegment();
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

	private void checkSegment(String readSegment) {
		if (appendingSegment == null || !appendingSegment.equals(readSegment)) {
			throw new IllegalStateException(
					"Append came in for a segment that was not the appending segment: " + readSegment);
		}
	}

}
