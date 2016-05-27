package com.emc.nautilus.common.netty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import lombok.Cleanup;

public class AppendEncodeDecodeTest {

	static {
		ResourceLeakDetector.setLevel(Level.ADVANCED);
	}

	private final class FakeLengthDecoder extends LengthFieldBasedFrameDecoder {
		FakeLengthDecoder() {
			super(1024 * 1024, 4, 4);
		}

		@Override
		protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
			return super.decode(ctx, in);
		}
	}

	private final String testStream = "Test Stream Name";
	private final CommandEncoder encoder = new CommandEncoder();
	private final FakeLengthDecoder lengthDecoder = new FakeLengthDecoder();
	private final CommandDecoder decoder = new CommandDecoder();

	@Test(expected = IllegalStateException.class)
	public void testAppendWithoutSetup() throws Exception {
		int size = 10;
		ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
		append(0, 1, size, fakeNetwork);
	}

	@Test
	public void testSwitchingStream() {
		fail();
	}

	@Test
	public void testFlushBeforeEndOfBlock() {
		fail();
	}

	@Test
	public void testFlushWhenAtBlockBoundry() {
		fail();
	}

	@Test
	public void testSmallAppends() throws Exception {
		int size = 10;
		ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
		ArrayList<Object> received = setupAppend(fakeNetwork);
		for (int i = 0; i < WireCommands.APPEND_BLOCK_SIZE; i++) {
			append(size * (i + 1), i, size, fakeNetwork);
			read(fakeNetwork, received);
		}
		assertEquals(size, received.size());
		verify(received, WireCommands.APPEND_BLOCK_SIZE, size);
	}

	@Test
	public void testMediumAppends() throws Exception {
		int size = 30000;
		ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
		ArrayList<Object> received = setupAppend(fakeNetwork);
		for (int i = 0; i < 5; i++) {
			append(size * (i + 1), i, size, fakeNetwork);
			read(fakeNetwork, received);
		}
		verify(received, 5, size);
	}

	@Test
	public void testAppendAtBlockBound() {
		fail();
	}

	@Test
	public void testAppendSpanningBlockBound() {
		fail();
	}

	@Test
	public void testLargeAppend() throws Exception {
		int size = 10 * WireCommands.APPEND_BLOCK_SIZE;
		ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
		ArrayList<Object> received = setupAppend(fakeNetwork);
		for (int i = 0; i < 2; i++) {
			append(size * (i + 1), i, size, fakeNetwork);
			read(fakeNetwork, received);
		}
		assertEquals(2, received.size());
		verify(received, 2, size);
	}

	private ArrayList<Object> setupAppend(ByteBuf fakeNetwork) throws Exception {
		SetupAppend setupAppend = new SetupAppend(new UUID(0, 0), testStream);
		encoder.encode(null, setupAppend, fakeNetwork);
		ArrayList<Object> received = new ArrayList<>();
		decoder.decode(null, fakeNetwork, received);
		assertEquals(1, received.size());
		assertEquals(setupAppend, received.remove(0));
		return received;
	}

	private long append(long offset, int contentValue, int length, ByteBuf out) throws Exception {
		@Cleanup("release")
		ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
		for (int i = 0; i < length; i++) {
			buffer.writeByte((byte) contentValue);
		}
		AppendData msg = new AppendData(testStream, offset, buffer);
		encoder.encode(null, msg, out);
		return offset + length;
	}

	private void read(ByteBuf in, List<Object> results) throws Exception {
		ByteBuf segmented = (ByteBuf) lengthDecoder.decode(null, in);
		while (segmented != null) {
			int before = results.size();
			decoder.decode(null, segmented, results);
			assertTrue(results.size() == before || results.size() == before + 1);
			segmented = (ByteBuf) lengthDecoder.decode(null, in);
		}
	}

	private void verify(List<Object> results, int numValues, int sizeOfEachValue) {
		int currentValue = -1;
		int currentCount = sizeOfEachValue;
		for (Object r : results) {
			AppendData append = (AppendData) r;
			assertEquals("Append split mid event", sizeOfEachValue, currentCount);
			while (append.data.isReadable()) {
				byte readByte = append.data.readByte();
				if (currentCount == sizeOfEachValue) {
					assertEquals((byte) (currentValue + 1), readByte);
					currentValue = currentValue + 1;
					currentCount = 1;
				} else {
					assertEquals((byte) currentValue, readByte);
					currentCount++;
				}
			}
		}
		assertEquals(numValues - 1, currentValue);
		assertEquals(currentCount, sizeOfEachValue);
	}

}
