package com.emc.logservice.server.handler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import com.emc.logservice.contracts.ConnectionInfo;
import com.emc.logservice.contracts.StreamSegmentNotExistsException;
import com.emc.logservice.contracts.StreamSegmentSealedException;
import com.emc.logservice.contracts.StreamSegmentStore;
import com.emc.logservice.contracts.WrongHostException;
import com.emc.nautilus.common.netty.DelegatingRequestProcessor;
import com.emc.nautilus.common.netty.RequestProcessor;
import com.emc.nautilus.common.netty.ServerConnection;
import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentIsSealed;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;

public class AppendProcessor extends DelegatingRequestProcessor {

	private static final Duration TIMEOUT = Duration.ofMinutes(1);
	static final int HIGH_WATER_MARK = 128 * 1024;
	static final int LOW_WATER_MARK = 64 * 1024;

	private final StreamSegmentStore store;
	private final ServerConnection connection;
	private final RequestProcessor next;

	private final Object lock = new Object();
	private String segment;
	private long connectionOffset = 0L;
	private List<ByteBuf> waiting = new ArrayList<>();
	private OutstandingWrite outstandingWrite = null;

	public AppendProcessor(StreamSegmentStore store, ServerConnection connection, RequestProcessor next) {
		this.store = store;
		this.connection = connection;
		this.next = next;
	}

	@Data
	private class OutstandingWrite {
		final ByteBuf[] dataToBeAppended;
		final String segment;
		final long ackOffset;
	}

	@Override
	public void setupAppend(SetupAppend setupAppend) {
		String newSegment = setupAppend.getSegment();
		String owner = store.whoOwnStreamSegment(newSegment);
		if (owner != null) {
			connection.sendAsync(new WrongHost(newSegment, owner));
			return;
		}
		UUID connectionId = setupAppend.getConnectionId();
		CompletableFuture<ConnectionInfo> future = store.getConnectionInfo(newSegment, connectionId);
		future.handle(new BiFunction<ConnectionInfo, Throwable, Void>() {
			@Override
			public Void apply(ConnectionInfo info, Throwable u) {
				if (info == null) {
					handleException(newSegment, u);
					return null;
				}
				if (!info.getSegment().equals(newSegment) || !info.getConnectionId().equals(connectionId)) {
					throw new IllegalStateException("Wrong connection Info returned");
				}
				long offset = info.getBytesWrittenSuccessfully();
				synchronized (lock) {
					segment = newSegment;
					connectionOffset = offset;
				}
				connection.sendAsync(new AppendSetup(newSegment, connectionId, connectionOffset));
				return null;
			}
		});
	}

	public void performNextWrite() {
		synchronized (lock) {
			if (outstandingWrite != null) {
				return;
			}
			ByteBuf[] data = new ByteBuf[waiting.size()];
			waiting.toArray(data);
			outstandingWrite = new OutstandingWrite(data, segment, connectionOffset);
			waiting.clear();
		}
		write(outstandingWrite);
	}

	private void write(OutstandingWrite toWrite) {
		ByteBuf buf = Unpooled.unmodifiableBuffer(toWrite.getDataToBeAppended());
		byte[] bytes = new byte[buf.readableBytes()];
		buf.readBytes(bytes);
		CompletableFuture<Long> future = store.append(toWrite.getSegment(), bytes, TIMEOUT);
		future.handle(new BiFunction<Long, Throwable, Void>() {
			@Override
			public Void apply(Long t, Throwable u) {
				synchronized (lock) {
					if (outstandingWrite != toWrite) {
						throw new IllegalStateException(
								"Synchronization error in: " + AppendProcessor.this.getClass().getName());
					}
					outstandingWrite = null;
					if (u != null) {
						segment = null;
						connectionOffset = 0;
						waiting.clear();
					}
				}
				if (t == null) {
					handleException(toWrite.getSegment(), u);
				} else {
					connection.sendAsync(new DataAppended(toWrite.segment, toWrite.ackOffset));
				}
				pauseOrResumeReading();
				performNextWrite();
				return null;
			}

		});
	}
	
	private void handleException(String segment, Throwable u) {
		if (u == null) {
			throw new IllegalStateException("Neither offset nor exception!?");
		}
		if (u instanceof StreamSegmentNotExistsException) {
			connection.sendAsync(new NoSuchSegment(segment));
		} else if (u instanceof StreamSegmentSealedException) {
			connection.sendAsync(new SegmentIsSealed(segment));
		} else if (u instanceof WrongHostException) {
			WrongHostException wrongHost = (WrongHostException) u;
			connection.sendAsync(new WrongHost(wrongHost.getStreamSegmentName(), wrongHost.getCorrectHost()));
		} else {
			//TODO: don't know what to do here...
			connection.drop();
			throw new IllegalStateException("Unknown exception.", u);
		}
	}

	private void pauseOrResumeReading() {
		int bytesWaiting;
		synchronized (lock) {
			bytesWaiting = waiting.stream().mapToInt((ByteBuf b) -> b.readableBytes()).sum();
		}
		if (bytesWaiting > HIGH_WATER_MARK) {
			connection.pauseReading();
		}
		if (bytesWaiting < LOW_WATER_MARK) {
			connection.resumeReading();
		}
	}

	@Override
	public void appendData(AppendData appendData) {
		synchronized (lock) {
			ByteBuf data = appendData.getData();
			long expectedOffset = appendData.getConnectionOffset();
			waiting.add(data);
			connectionOffset += data.readableBytes();
			if (connectionOffset != expectedOffset) {
				throw new IllegalStateException("Data appended so far was not of the expected length.");
			}
		}
		pauseOrResumeReading();
		performNextWrite();
	}

	@Override
	public RequestProcessor getNextRequestProcessor() {
		return next;
	}

}
