package com.emc.nautilus.integrationtests;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.emc.logservice.server.containers.StreamSegmentContainer;
import com.emc.logservice.server.handler.AppendProcessor;
import com.emc.logservice.server.handler.LogSerivceConnectionListener;
import com.emc.logservice.server.handler.LogServiceRequestProcessor;
import com.emc.logservice.server.handler.ServerConnectionInboundHandler;
import com.emc.nautilus.common.netty.CommandDecoder;
import com.emc.nautilus.common.netty.CommandEncoder;
import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.Reply;
import com.emc.nautilus.common.netty.Request;
import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.CreateSegment;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.common.netty.client.ConnectionFactoryImpl;
import com.emc.nautilus.logclient.LogAppender;
import com.emc.nautilus.logclient.LogOutputStream;
import com.emc.nautilus.logclient.LogOutputStream.AckListener;
import com.emc.nautilus.logclient.impl.LogClientImpl;
import com.emc.nautilus.streaming.Producer;
import com.emc.nautilus.streaming.ProducerConfig;
import com.emc.nautilus.streaming.Stream;
import com.emc.nautilus.streaming.impl.JavaSerializer;
import com.emc.nautilus.streaming.impl.SingleLogStreamManagerImpl;

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
		StreamSegmentContainer store = LogSerivceConnectionListener.createStore("ABC");

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
		StreamSegmentContainer store = LogSerivceConnectionListener.createStore("ABC");

		EmbeddedChannel channel = createChannel(store);
		CommandDecoder decoder = new CommandDecoder();

		SegmentCreated created = (SegmentCreated) sendRequest(channel, decoder, new CreateSegment(segment));
		assertEquals(segment, created.getSegment());

		UUID uuid = UUID.randomUUID();
		AppendSetup setup = (AppendSetup) sendRequest(channel, decoder, new SetupAppend(uuid, segment));

		assertEquals(segment, setup.getSegment());
		assertEquals(uuid, setup.getConnectionId());

		DataAppended ack = (DataAppended) sendRequest(	channel,
														decoder,
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
		ServerConnectionInboundHandler lsh = new ServerConnectionInboundHandler();
		lsh.setRequestProcessor(new AppendProcessor(store, lsh, new LogServiceRequestProcessor(store, lsh)));
		EmbeddedChannel channel = new EmbeddedChannel(new CommandEncoder(),
				new LengthFieldBasedFrameDecoder(1024 * 1024, 4, 4),
				new CommandDecoder(),
				lsh);
		return channel;
	}

	@Test
	public void appendThroughLogClient() throws Exception {
		String endpoint = "localhost";
		String logName = "abc";
		int port = 8765;
		String testString = "Hello world\n";
		@Cleanup("shutdown")
		LogSerivceConnectionListener server = new LogSerivceConnectionListener(false, port, "My ContainerId");
		server.startListening();

		ConnectionFactory clientCF = new ConnectionFactoryImpl(false, port);
		LogClientImpl logClient = new LogClientImpl(endpoint, clientCF);
		logClient.createLog(logName);
		@Cleanup("close")
		LogAppender appender = logClient.openLogForAppending(logName, null);
		@Cleanup("close")
		LogOutputStream out = appender.getOutputStream();
		CompletableFuture<Long> ack = new CompletableFuture<Long>();
		out.setWriteAckListener(new AckListener() {
			@Override
			public void ack(long connectionOffset) {
				ack.complete(connectionOffset);
			}
		});
		out.write(ByteBuffer.wrap(testString.getBytes()));
		out.flush();
		assertEquals(testString.length(), ack.get(5, TimeUnit.SECONDS).longValue());
	}

	@Test
	public void appendThroughStreamingClient() {
		String endpoint = "localhost";
		String streamName = "abc";
		int port = 8910;
		String testString = "Hello world\n";
		@Cleanup("shutdown")
		LogSerivceConnectionListener server = new LogSerivceConnectionListener(false, port, "My ContainerId");
		server.startListening();
		ConnectionFactory clientCF = new ConnectionFactoryImpl(false, port);
		LogClientImpl logClient = new LogClientImpl(endpoint, clientCF);
		SingleLogStreamManagerImpl streamManager = new SingleLogStreamManagerImpl("Scope", logClient);
		Stream stream = streamManager.createStream(streamName, null);
		@Cleanup
		Producer<String> producer = stream.createProducer(new JavaSerializer<>(), new ProducerConfig(null));
		producer.publish("RoutingKey", testString);
		producer.flush();
	}
}
