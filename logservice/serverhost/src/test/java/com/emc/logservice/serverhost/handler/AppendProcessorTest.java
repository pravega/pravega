package com.emc.logservice.serverhost.handler;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import com.emc.logservice.contracts.AppendContext;
import com.emc.logservice.contracts.StreamSegmentStore;
import com.emc.nautilus.common.netty.FailingRequestProcessor;
import com.emc.nautilus.common.netty.ServerConnection;
import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;

import io.netty.buffer.Unpooled;

public class AppendProcessorTest {

	@Test
	public void testAppend() {
		String streamSegmentName = "testAppendSegment";
		UUID clientId = UUID.randomUUID();
		byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
		StreamSegmentStore store = mock(StreamSegmentStore.class);
		ServerConnection connection = mock(ServerConnection.class);
		AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor());

		CompletableFuture<AppendContext> contextFuture = new CompletableFuture<>();
		contextFuture.complete(new AppendContext(clientId, 0));
		when(store.getLastAppendContext(streamSegmentName, clientId)).thenReturn(contextFuture);
		CompletableFuture<Long> result = new CompletableFuture<Long>();
		result.complete((long) data.length);
		when(store.append(streamSegmentName, data, new AppendContext(clientId, data.length), AppendProcessor.TIMEOUT))
			.thenReturn(result);

		processor.setupAppend(new SetupAppend(clientId, streamSegmentName));
		processor.appendData(new AppendData(clientId, data.length, Unpooled.wrappedBuffer(data)));
		verify(store).getLastAppendContext(anyString(), any());
		verify(store).append(	streamSegmentName,
								data,
								new AppendContext(clientId, data.length),
								AppendProcessor.TIMEOUT);
		verify(connection).send(new AppendSetup(streamSegmentName, clientId, 0));
		verify(connection, atLeast(0)).resumeReading();
		verify(connection).send(new DataAppended(streamSegmentName, data.length));
		verifyNoMoreInteractions(connection);
		verifyNoMoreInteractions(store);
	}

	@Test
	public void testInvalidOffset() {
		fail();
	}

	@Test
	public void testSwitchingStream() {
		fail();
	}

	@Test
	public void testAppendFails() {
		String streamSegmentName = "testAppendSegment";
		UUID clientId = UUID.randomUUID();
		byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
		StreamSegmentStore store = mock(StreamSegmentStore.class);
		ServerConnection connection = mock(ServerConnection.class);
		AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor());

		CompletableFuture<AppendContext> contextFuture = new CompletableFuture<>();
		contextFuture.complete(new AppendContext(clientId, 0));
		when(store.getLastAppendContext(streamSegmentName, clientId)).thenReturn(contextFuture);
		CompletableFuture<Long> result = new CompletableFuture<Long>();
		result.completeExceptionally(new RuntimeException());
		when(store.append(streamSegmentName, data, new AppendContext(clientId, data.length), AppendProcessor.TIMEOUT))
			.thenReturn(result);

		processor.setupAppend(new SetupAppend(clientId, streamSegmentName));
		processor.appendData(new AppendData(clientId, data.length, Unpooled.wrappedBuffer(data)));
		try {
			processor.appendData(new AppendData(clientId, data.length * 2, Unpooled.wrappedBuffer(data)));
			fail();
		} catch (IllegalStateException e) {
			// Expected
		}
		verify(connection).send(new AppendSetup(streamSegmentName, clientId, 0));
		verify(connection, atLeast(0)).resumeReading();
		verify(connection).drop();
		verify(store, atMost(1)).append(any(), any(), any(), any());
		verifyNoMoreInteractions(connection);
	}

	@Test
	public void testRecoveryFromFailure() {
		fail();
	}
}
