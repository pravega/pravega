package com.emc.nautilus.logclient.impl;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.emc.nautilus.common.netty.ClientConnection;
import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.FailingReplyProcessor;
import com.emc.nautilus.common.netty.WireCommands.CreateSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.nautilus.common.netty.WireCommands.SegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;
import com.emc.nautilus.logclient.LogServiceClient;
import com.emc.nautilus.logclient.SegmentInputConfiguration;
import com.emc.nautilus.logclient.SegmentInputStream;
import com.emc.nautilus.logclient.SegmentOutputConfiguration;
import com.emc.nautilus.logclient.SegmentOutputStream;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

@RequiredArgsConstructor
public class LogServiceClientImpl implements LogServiceClient {

	private final String endpoint;
	private final ConnectionFactory connectionFactory;
	
	@Override
	@Synchronized
	public boolean createSegment(String name) {
		ClientConnection connection = connectionFactory.establishConnection(endpoint);
		CompletableFuture<Boolean> result = new CompletableFuture<>();
		connection.setResponseProcessor(new FailingReplyProcessor() {
			@Override
			public void wrongHost(WrongHost wrongHost) {
				result.completeExceptionally(new UnsupportedOperationException("TODO"));
			}
			@Override
			public void segmentAlreadyExists(SegmentAlreadyExists segmentAlreadyExists) {
				result.complete(false);
			}
			
			@Override
			public void segmentCreated(SegmentCreated segmentCreated) {
				result.complete(true);
			}
		});
		connection.send(new CreateSegment(name));
		try {
			return result.get();
		} catch (ExecutionException e) {
			throw new RuntimeException(e.getCause());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean segmentExists(String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public SegmentOutputStream openSegmentForAppending(String name, SegmentOutputConfiguration config) {
		return new SegmentOutputStreamImpl(connectionFactory, endpoint, UUID.randomUUID(), name);
	}

	@Override
	public SegmentInputStream openLogForReading(String name, SegmentInputConfiguration config) {
		return new SegmentInputStreamImpl(new AsyncSegmentInputStreamImpl(connectionFactory, endpoint, name));
	}

	@Override
	public SegmentOutputStream openTransactionForAppending(String name, UUID txId) {
		throw new UnsupportedOperationException();
	}

}
