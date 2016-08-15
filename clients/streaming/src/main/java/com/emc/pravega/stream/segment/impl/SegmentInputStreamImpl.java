/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.segment.impl;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.util.CircularBuffer;
import com.emc.pravega.stream.segment.EndOfSegmentException;
import com.emc.pravega.stream.segment.SegmentInputStream;
import com.google.common.base.Preconditions;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

/**
 * Manages buffering and provides a synchronus to {@link AsyncSegmentInputStream}
 * @see SegmentInputStream
 */
@RequiredArgsConstructor
public class SegmentInputStreamImpl extends SegmentInputStream {

	private final AsyncSegmentInputStream asyncInput;
	private static final int READ_LENGTH = 1024 * 1024;
	private final CircularBuffer buffer = new CircularBuffer(2 * READ_LENGTH);
	private long offset = 0;
	private boolean receivedEndOfSegment = false;
	private Future<SegmentRead> outstandingRequest = null;

	@Override
	@Synchronized
	public void setOffset(long offset) {
		this.offset = offset;
		buffer.clear();
		receivedEndOfSegment = false;
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

	/**
	 * @see com.emc.pravega.stream.segment.SegmentInputStream#read(java.nio.ByteBuffer)
	 */
	@Override
	@Synchronized
	public int read(ByteBuffer toFill) throws EndOfSegmentException {
	    Preconditions.checkNotNull(toFill);
		issueRequestIfNeeded();
		if (outstandingRequest.isDone() || buffer.dataAvailable() <= 0) {
			try {
				handleRequest();
			} catch (ExecutionException e) {
				throw new RuntimeException(e.getCause());
			}
		}
		if (buffer.dataAvailable() <= 0 && receivedEndOfSegment) {
			throw new EndOfSegmentException();
		}
		int read = buffer.read(toFill);
		offset += read;
		return read;
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
		if (segmentRead.isEndOfSegment()) {
			receivedEndOfSegment = true;
		}
		if (!segmentRead.getData().hasRemaining()) {
			outstandingRequest = null;
			issueRequestIfNeeded();
		}
	}

	/**
	 * @return If there is enough room for another request, and we aren't already waiting on one
	 */
	private void issueRequestIfNeeded() {
		if (!receivedEndOfSegment && outstandingRequest == null && buffer.capacityAvailable() > READ_LENGTH) {
			outstandingRequest = asyncInput.read(offset + buffer.dataAvailable(), READ_LENGTH);
		}
	}

	@Override
	@Synchronized
	public void close() {
		asyncInput.close();
	}

}
