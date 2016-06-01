package com.emc.nautilus.logclient.impl;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.emc.nautilus.common.netty.ClientConnection;
import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.FailingReplyProcessor;
import com.emc.nautilus.common.netty.WireCommands.CreateSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.nautilus.common.netty.WireCommands.SegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;
import com.emc.nautilus.logclient.Batch;
import com.emc.nautilus.logclient.LogAppender;
import com.emc.nautilus.logclient.LogClient;
import com.emc.nautilus.logclient.LogInputConfiguration;
import com.emc.nautilus.logclient.LogInputStream;
import com.emc.nautilus.logclient.LogOutputConfiguration;
import com.emc.nautilus.logclient.LogOutputStream;

import lombok.Synchronized;

public class LogClientImpl implements LogClient {

	ConnectionFactory connectionFactory;
	String endpoint;
	
	@Override
	@Synchronized
	public boolean createLog(String name, long timeoutMillis) {
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
		connection.sendAsync(new CreateSegment(name));
		return result.get(timeoutMillis, TimeUnit.MILLISECONDS);
	}

	@Override
	public boolean logExists(String name, long timeoutMillis) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public LogAppender openLogForAppending(String name, LogOutputConfiguration config) {
		return new LogAppender() {
			@Override
			public void close() throws Exception {
				throw new UnsupportedOperationException("TODO");
			}
			@Override
			public LogOutputStream getOutputStream() {
				return new LogOutputStreamImpl(connectionFactory, endpoint, UUID.randomUUID(), name);
			}
			
			@Override
			public Future<Long> seal(long timeoutMillis) {
				throw new UnsupportedOperationException("TODO");
			}
			@Override
			public Batch createBatch(long timeoutMillis) {
				throw new UnsupportedOperationException("TODO");
			}
		};
	}

	@Override
	public LogInputStream openLogForReading(String name, LogInputConfiguration config) {
		return new LogInputStreamImpl(new AsyncLogInputStreamImpl(connectionFactory, endpoint, name));
	}

}
