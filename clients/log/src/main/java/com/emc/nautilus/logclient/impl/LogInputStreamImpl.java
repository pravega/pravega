package com.emc.nautilus.logclient.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import com.emc.nautilus.common.netty.ClientConnection;
import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.ConnectionFailedException;
import com.emc.nautilus.common.netty.FailingResponseProcessor;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.ReadSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentRead;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;

public class LogInputStreamImpl extends AsyncLogInputStream {

	private final ConnectionFactory connectionFactory;
	private final String endpoint;
	private final String segment;
	private final AtomicReference<ClientConnection> connection = new AtomicReference<>();
	private final ConcurrentHashMap<Long, CompletableFuture<SegmentRead>> outstandingRequests = new ConcurrentHashMap<>();
	private final ResponseProcessor responseProcessor = new ResponseProcessor();
	
	private final class ResponseProcessor extends FailingResponseProcessor {

		public void wrongHost(WrongHost wrongHost) {
			reconnect(new ConnectionFailedException(wrongHost.toString()));
		}

		public void noSuchSegment(NoSuchSegment noSuchSegment) {
			reconnect(new IllegalArgumentException(noSuchSegment.toString()));
		}

		public void segmentRead(SegmentRead segmentRead) {
			CompletableFuture<SegmentRead> future = outstandingRequests.remove(segmentRead.getOffset());
			if (future != null) {
				future.complete(segmentRead);
			}
		}
	}

	public LogInputStreamImpl(ConnectionFactory connectionFactory, String endpoint, String segment) {
		super();
		this.connectionFactory = connectionFactory;
		this.endpoint = endpoint;
		this.segment = segment;
		reconnect(null);
	}

	private void reconnect(Exception e) { //TODO: we need backoff
		ClientConnection newConnection = connectionFactory.establishConnection(endpoint);
		newConnection.setResponseProcessor(responseProcessor);
		ClientConnection oldConnection = connection.getAndSet(newConnection);
		if (oldConnection != null) {
			oldConnection.drop();
		}
		List<Entry<Long, CompletableFuture<SegmentRead>>> outstanding = new ArrayList<>(
				outstandingRequests.entrySet()); //TODO: Is there a way not to copy this without a race?
		for (Entry<Long, CompletableFuture<SegmentRead>> read : outstanding) {
			read.getValue().completeExceptionally(e);
			outstandingRequests.remove(read.getKey(), read.getValue());
		}
	}

	@Override
	public void close() {
		ClientConnection c = connection.getAndSet(null);
		if (c != null) {
			c.drop();
		}
	}

	@Override
	public Future<SegmentRead> read(long offset, int length) {
		ClientConnection c = connection.get();
		if (c == null) {
			throw new IllegalStateException("Not connected");
		}
		CompletableFuture<SegmentRead> future = new CompletableFuture<>();
		outstandingRequests.put(offset, future);
		c.sendAsync(new ReadSegment(segment, offset, length));
		return future;
	}

}
