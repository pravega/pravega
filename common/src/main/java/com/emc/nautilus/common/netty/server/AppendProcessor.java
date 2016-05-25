package com.emc.nautilus.common.netty.server;

import java.util.ArrayList;
import java.util.List;

import com.emc.nautilus.common.netty.CommandProcessor;
import com.emc.nautilus.common.netty.Connection;
import com.emc.nautilus.common.netty.DelegatingCommandProcessor;
import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;

import io.netty.buffer.ByteBuf;
import lombok.Data;

public class AppendProcessor extends DelegatingCommandProcessor {

	static final int HIGH_WATER_MARK = 1024*1024;
	static final int LOW_WATER_MARK = 512*1024;
	
	private final CommandProcessor next;
	private final Connection connection;
	
	private final Object lock = new Object();
	private String segment;
	private long connectionOffset = 0L;
	private List<ByteBuf> waiting = new ArrayList<>();
	private OutstandingWrite outstandingWrite = null;

	AppendProcessor(Connection connection, CommandProcessor next) {
		this.connection = connection;
		this.next = next;
	}
	
	@Data
	private class OutstandingWrite {
		final List<ByteBuf> dataToBeAppended;
		final String segment;
		final long ackOffset;

		public void complete() {
			connection.asyncSend(new DataAppended(segment, ackOffset));
			pauseOrResumeReading();
			synchronized (lock) {
				outstandingWrite = null;
			}
			performNextWrite();
		}		
	}
	
	@Override
	public void setupAppend(SetupAppend setupAppend) {
		String segment = setupAppend.getSegment();
		if (!ownSegment(segment)) {
			connection.asyncSend(new WrongHost());
		}
		synchronized (lock) {
			this.segment = segment;
			connectionOffset = 0L;
		}
		connection.asyncSend(new AppendSetup(segment));
	}

	public void performNextWrite() {
		synchronized (lock) {
			if (outstandingWrite != null) {
				return;
			}
			outstandingWrite = new OutstandingWrite(waiting, segment, connectionOffset);
			waiting = new ArrayList<>();
		}
		write(outstandingWrite);
	}

	private void write(OutstandingWrite toWrite) {
		// TODO Auto-generated method stub
		//Make sure on completion toWrite's complete() method is called.
	}

	private void pauseOrResumeReading() {
		int bytesWaiting;
		synchronized (lock) {
			bytesWaiting = waiting.stream().mapToInt((ByteBuf b)->b.readableBytes()).sum();
		}
		if (bytesWaiting > HIGH_WATER_MARK) {
			connection.pauseReading();
		}
		if (bytesWaiting < LOW_WATER_MARK) {
			connection.resumeReading();
		}
	}
	
	
	private boolean ownSegment(String name) {
		// TODO Auto-generated method stub
		return false;
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
	public CommandProcessor getNextCommandProcessor() {
		return next;
	}
	
}
