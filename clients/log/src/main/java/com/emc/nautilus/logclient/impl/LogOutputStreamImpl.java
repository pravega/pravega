package com.emc.nautilus.logclient.impl;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListMap;

import com.emc.nautilus.common.netty.Connection;
import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.ConnectionFailedException;
import com.emc.nautilus.common.utils.RetryUtil;
import com.emc.nautilus.common.utils.RetryUtil.RecoverableTask;
import com.emc.nautilus.logclient.LogOutputStream;
import com.emc.nautilus.logclient.LogSealedExcepetion;

import lombok.Synchronized;

public class LogOutputStreamImpl extends LogOutputStream {

	ConnectionFactory connectionFactory;
	private String endpoint;
	Connection connection;
	ConcurrentSkipListMap<Long, ByteBuffer> inflight;
	AckListener ackListener;
	long writePointer = 0;
	long ackPointer = 0;
	
	@Override
	public void setWriteAckListener(AckListener callback) {
		ackListener = callback;
	}

	void onAck(long offset) {
		
	}
	
	@Override
	@Synchronized
	public long write(ByteBuffer buff) throws LogSealedExcepetion {
		writePointer += buff.remaining();
		inflight.put(writePointer, buff);
		RetryUtil.retryWithBackoff(new RecoverableTask<ConnectionFailedException>() {
			@Override
			public void attempt() throws ConnectionFailedException {
				writeToChannel(buff);
			}
			@Override
			public void recover() {
				reestablishConnection();
			}
		});
		return writePointer;
	}

	private void writeToChannel(ByteBuffer buff) throws ConnectionFailedException {
		Integer location = computeHeaderPos(buff.remaining());
		if (location == null){
			connection.write(buff);
		} else {
			int limit = buff.limit();
			buff.limit(buff.position()+location);
			connection.write(buff);
			buff.limit(limit);
			writeHeader(buff.remaining());
			connection.write(buff);
		}
	}

	private Integer computeHeaderPos(int remaining) {
		// TODO Auto-generated method stub
		return null;
	}

	private void writeHeader(int remaining) {
		// TODO Auto-generated method stub
		
	}

	private void reestablishConnection() {
		connection = connectionFactory.establishConnection(endpoint);
		sendRequestToResumePublishing();
		long level = readWhatWasReceived();
		onAck(level);
		retransmitInflight();
	}

	private void retransmitInflight() {
		// TODO Auto-generated method stub
		
	}

	private long readWhatWasReceived() {
		// TODO Auto-generated method stub
		return 0;
	}

	private void sendRequestToResumePublishing() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws LogSealedExcepetion {
		// TODO Auto-generated method stub

	}

	@Override
	public void flush() throws LogSealedExcepetion {
		// TODO Auto-generated method stub

	}

}
