package com.emc.nautilus.common.netty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.KeepAlive;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import lombok.Cleanup;

public class AppendEncodeDecodeTest {

	private Level origionalLevel;

	@Before
	public void setup() {
		origionalLevel = ResourceLeakDetector.getLevel();
		ResourceLeakDetector.setLevel(Level.PARANOID);
	}

	@After
	public void teardown() {
		ResourceLeakDetector.setLevel(origionalLevel);
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
		@Cleanup("release")
		ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
		append(0, 1, size, fakeNetwork);
	}

	@Test
	public void testSwitchingStream() {
		fail();
	}

	@Test
	public void testFlushBeforeEndOfBlock() throws Exception {
		testFlush(WireCommands.APPEND_BLOCK_SIZE / 2);
	}

	@Test
	public void testFlushWhenAtBlockBoundry() throws Exception {
		testFlush(WireCommands.APPEND_BLOCK_SIZE);
	}

	private void testFlush(int size) throws Exception {
		@Cleanup("release")
		ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
		ArrayList<Object> received = setupAppend(fakeNetwork);

		append(size, 0, size, fakeNetwork);
		read(fakeNetwork, received);

		KeepAlive keepAlive = new KeepAlive();
		encoder.encode(null, keepAlive, fakeNetwork);
		read(fakeNetwork, received);
		assertEquals(2, received.size());

		AppendData one = (AppendData) received.get(0);
		assertEquals(size, one.data.readableBytes());
		KeepAlive two = (KeepAlive) received.get(1);
		assertEquals(keepAlive, two);
	}

	@Test
	public void testSmallAppends() throws Exception {
		int size = 10;
		@Cleanup("release")
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
	public void testAppendSpanningBlockBound() throws Exception {
		int size = (WireCommands.APPEND_BLOCK_SIZE * 3) / 4;
		@Cleanup("release")
		ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
		ArrayList<Object> received = setupAppend(fakeNetwork);
		for (int i = 0; i < 4; i++) {
			append(size * (i + 1), i, size, fakeNetwork);
			read(fakeNetwork, received);
		}
		verify(received, 4, size);
	}

	@Test
	public void testBlockSizeAppend() throws Exception {
		int size = WireCommands.APPEND_BLOCK_SIZE;
		@Cleanup("release")
		ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
		ArrayList<Object> received = setupAppend(fakeNetwork);
		for (int i = 0; i < 4; i++) {
			append(size * (i + 1), i, size, fakeNetwork);
			read(fakeNetwork, received);
		}
		assertEquals(4, received.size());
		verify(received, 4, size);
	}

	@Test
	public void testAppendAtBlockBound() throws Exception {
		int size = WireCommands.APPEND_BLOCK_SIZE;
		@Cleanup("release")
		ByteBuf fakeNetwork = ByteBufAllocator.DEFAULT.buffer();
		ArrayList<Object> received = setupAppend(fakeNetwork);

		append(size, 1, size, fakeNetwork);
		read(fakeNetwork, received);
		assertEquals(1, received.size());

		append(size + size / 2, 2, size / 2, fakeNetwork);
		read(fakeNetwork, received);
		assertEquals(1, received.size());

		KeepAlive keepAlive = new KeepAlive();
		encoder.encode(null, keepAlive, fakeNetwork);
		read(fakeNetwork, received);
		assertEquals(3, received.size());

		AppendData one = (AppendData) received.get(0);
		AppendData two = (AppendData) received.get(1);
		assertEquals(size, one.data.readableBytes());
		assertEquals(size / 2, two.data.readableBytes());
		KeepAlive three = (KeepAlive) received.get(2);
		assertEquals(keepAlive, three);
	}

	@Test
	public void testLargeAppend() throws Exception {
		int size = 10 * WireCommands.APPEND_BLOCK_SIZE;
		@Cleanup("release")
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
		byte[] content = new byte[length];
		Arrays.fill(content, (byte) contentValue);
		ByteBuf buffer = Unpooled.wrappedBuffer(content);
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
		int readSoFar = 0;
		int currentValue = -1;
		int currentCount = sizeOfEachValue;
		for (Object r : results) {
			AppendData append = (AppendData) r;
			assertEquals("Append split mid event", sizeOfEachValue, currentCount);
			readSoFar += append.getData().readableBytes();
			assertEquals(readSoFar, append.getConnectionOffset());
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
