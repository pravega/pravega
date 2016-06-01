package com.emc.nautilus.integrationtests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.emc.logservice.server.containers.StreamSegmentContainer;
import com.emc.logservice.server.handler.AppendProcessor;
import com.emc.logservice.server.handler.LogSerivceServer;
import com.emc.logservice.server.handler.LogServiceRequestProcessor;
import com.emc.logservice.server.handler.LogServiceServerHandler;
import com.emc.nautilus.common.netty.CommandDecoder;
import com.emc.nautilus.common.netty.CommandEncoder;
import com.emc.nautilus.common.netty.Reply;
import com.emc.nautilus.common.netty.Request;
import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.CreateSegment;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.logclient.impl.LogClientImpl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import lombok.Cleanup;

public class AppendTest {
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
	
	@Test
	public void testSetupOnNonExistentSegment() throws Exception {
		String segment = "123";
		ByteBuf data = Unpooled.wrappedBuffer("Hello world\n".getBytes());
		@Cleanup
		StreamSegmentContainer store = LogSerivceServer.createStore("ABC");
		store.initialize(Duration.ofMinutes(1)).get();
		store.start(Duration.ofMinutes(1)).get();

		EmbeddedChannel channel = createChannel(store);
		CommandDecoder decoder = new CommandDecoder();

		UUID uuid = UUID.randomUUID();
		NoSuchSegment setup = (NoSuchSegment) sendRequest(channel, decoder, new SetupAppend(uuid, segment));

		assertEquals(segment, setup.getSegment());
	}

	@Test
	public void sendReceivingAppend() throws Exception {
		String segment = "123";
		ByteBuf data = Unpooled.wrappedBuffer("Hello world\n".getBytes());
		@Cleanup
		StreamSegmentContainer store = LogSerivceServer.createStore("ABC");
		store.initialize(Duration.ofMinutes(1)).get();
		store.start(Duration.ofMinutes(1)).get();

		EmbeddedChannel channel = createChannel(store);
		CommandDecoder decoder = new CommandDecoder();

		SegmentCreated created = (SegmentCreated) sendRequest(channel, decoder, new CreateSegment(segment));
		assertEquals(segment, created.getSegment());
		
		UUID uuid = UUID.randomUUID();
		AppendSetup setup = (AppendSetup) sendRequest(channel, decoder, new SetupAppend(uuid, segment));

		assertEquals(segment, setup.getSegment());
		assertEquals(uuid, setup.getConnectionId());

		DataAppended ack = (DataAppended) sendRequest(channel, decoder,
				new AppendData(segment, data.readableBytes(), data));
		assertEquals(segment, ack.getSegment());
		assertEquals(data.readableBytes(), ack.getConnectionOffset());
	}

	private Reply sendRequest(EmbeddedChannel channel, CommandDecoder decoder, Request request) throws Exception {
		ArrayList<Object> decoded = new ArrayList<>();
		channel.writeInbound(request);
		Object encodedReply = channel.readOutbound();
		for (int i = 0; encodedReply == null && i < 50; i++) {
			Thread.sleep(100);
			encodedReply = channel.readOutbound();
		}
		if (encodedReply == null) {
			throw new IllegalStateException("No reply to request: " + request);
		}
		decoder.decode((ByteBuf) encodedReply, decoded);
		assertEquals(1, decoded.size());
		return (Reply) decoded.get(0);
	}

	private EmbeddedChannel createChannel(StreamSegmentContainer store) {
		LogServiceServerHandler lsh = new LogServiceServerHandler();
		lsh.setRequestProcessor(new AppendProcessor(store, lsh, new LogServiceRequestProcessor(store, lsh)));
		EmbeddedChannel channel = new EmbeddedChannel(new CommandEncoder(),
				new LengthFieldBasedFrameDecoder(1024 * 1024, 4, 4), new CommandDecoder(), lsh);
		return channel;
	}

	@Test
	public void sendAppendOverWire() {
		fail();
	}

	@Test
	public void appendThroughLogClient() {
		LogClientImpl logClient = new LogClientImpl();
		fail();
	}

	@Test
	public void appendThroughStreamingClient() {
		fail();
	}
}
