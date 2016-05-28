package com.emc.nautilus.logclient.impl;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.emc.nautilus.common.netty.WireCommands.SegmentRead;
import com.emc.nautilus.common.utils.CircularBuffer;
import com.emc.nautilus.logclient.EndOfLogException;
import com.emc.nautilus.logclient.LogInputStream;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

@RequiredArgsConstructor
public class LogInputStreamImpl extends LogInputStream {

	private final AsyncLogInputStream asyncLogInput;
	private static final int READ_LENGTH = 1024 * 1024;
	private final LogInputStream input;
	private final CircularBuffer buffer = new CircularBuffer(2 * READ_LENGTH);
	private long offset = 0;
	private boolean receivedEndOfStream = false;
	private Future<SegmentRead> outstandingRequest = null;

	@Override
	@Synchronized
	public void setOffset(long offset) {
		this.offset = offset;
		buffer.clear();
		receivedEndOfStream = false;
	}

	@Override
	@Synchronized
	public long getOffset() {
		return offset;
	}

	@Override
	@Synchronized
	public int available() {
		return buffer.dataAvailable();
	}

	@Override
	@Synchronized
	public void read(ByteBuffer toFill) throws EndOfLogException {
		issueRequestIfNeeded();
		if (outstandingRequest.isDone() || buffer.dataAvailable() <= 0) {
			try {
				handleRequest();
			} catch (ExecutionException e) {
				throw new RuntimeException(e.getCause());
			}
		}
		if (buffer.dataAvailable() <= 0 && receivedEndOfStream) {
			throw new EndOfLogException();
		}
		offset += buffer.read(toFill);
	}

	private void handleRequest() throws ExecutionException {
		SegmentRead segmentRead;
		try {
			segmentRead = outstandingRequest.get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
		if (segmentRead.getData().hasRemaining()) {
			buffer.fill(segmentRead.getData());
		}
		if (segmentRead.isEndOfStream()) {
			receivedEndOfStream = true;
		}
		if (!segmentRead.getData().hasRemaining()) {
			outstandingRequest = null;
			issueRequestIfNeeded();
		}
	}

	private void issueRequestIfNeeded() {
		if (!receivedEndOfStream && outstandingRequest == null && buffer.capacityAvailable() > READ_LENGTH) {
			outstandingRequest = asyncLogInput.read(offset + buffer.dataAvailable(), READ_LENGTH);
		}
	}

	@Override
	@Synchronized
	public void close() {
		input.close();
	}

}
