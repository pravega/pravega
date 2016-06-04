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
import com.emc.nautilus.logclient.LogClient;
import com.emc.nautilus.logclient.LogInputConfiguration;
import com.emc.nautilus.logclient.LogInputStream;
import com.emc.nautilus.logclient.LogOutputConfiguration;
import com.emc.nautilus.logclient.LogOutputStream;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

@RequiredArgsConstructor
public class LogClientImpl implements LogClient {

	private final String endpoint;
	private final ConnectionFactory connectionFactory;
	
	@Override
	@Synchronized
	public boolean createLog(String name) {
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
	public boolean logExists(String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public LogOutputStream openLogForAppending(String name, LogOutputConfiguration config) {
		return new LogOutputStreamImpl(connectionFactory, endpoint, UUID.randomUUID(), name);
	}

	@Override
	public LogInputStream openLogForReading(String name, LogInputConfiguration config) {
		return new LogInputStreamImpl(new AsyncLogInputStreamImpl(connectionFactory, endpoint, name));
	}

	@Override
	public LogOutputStream openTransactionForAppending(String name, UUID txId) {
		throw new UnsupportedOperationException();
	}

}
