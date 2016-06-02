package com.emc.nautilus.common.netty;

import static com.emc.nautilus.common.netty.WireCommands.APPEND_BLOCK_SIZE;

import java.io.IOException;

import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.common.netty.WireCommands.Type;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.SneakyThrows;

public class CommandEncoder extends MessageToByteEncoder<WireCommand> {
	private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

	private String segment;
	private long connectionOffset;
	private boolean headerOnNextAppend;

	@Override
	protected void encode(ChannelHandlerContext ctx, WireCommand msg, ByteBuf out) throws Exception {
		switch (msg.getType()) {
		case APPEND_DATA:
			AppendData append = (AppendData) msg;
			if (segment == null || !segment.equals(append.getSegment())) {
				throw new IllegalStateException("Sending appends without setting up the append.");
			}
			ByteBuf data = append.getData();
			int dataLength = data.readableBytes();
			if (append.getConnectionOffset() != connectionOffset + dataLength) {
				throw new IllegalStateException("Connection offset in append not of expected length: "
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
			segment = ((SetupAppend) msg).getSegment();
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
		bout.writeInt(Type.APPEND_DATA_HEADER.getCode());
		bout.write(LENGTH_PLACEHOLDER);
		bout.writeLong(connectionOffset);
		bout.writeUTF(segment);
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
		bout.writeInt(Type.APPEND_DATA_FOOTER.getCode());
		bout.write(LENGTH_PLACEHOLDER);
		bout.writeLong(connectionOffset);
		bout.writeInt(dataLength);
		bout.writeUTF(segment);
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
			long dataInBlock = connectionOffset - (connectionOffset / APPEND_BLOCK_SIZE) * (APPEND_BLOCK_SIZE);
			if (dataInBlock > 0) {
				int remainingInBlock = (int) (APPEND_BLOCK_SIZE - dataInBlock);
				out.writeZero(remainingInBlock);
				writeFooter(null, -remainingInBlock, out);
			}
			headerOnNextAppend = true;
		}
		segment = null;
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
