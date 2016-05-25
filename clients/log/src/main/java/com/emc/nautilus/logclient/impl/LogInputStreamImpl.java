package com.emc.nautilus.logclient.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.emc.nautilus.common.netty.Connection;
import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.ConnectionFailedException;
import com.emc.nautilus.common.netty.WireCommands.SegmentRead;
import com.emc.nautilus.common.utils.RetryUtil;
import com.emc.nautilus.common.utils.RetryUtil.RecoverableTask;
import com.emc.nautilus.common.utils.RetryUtil.Task;
import com.emc.nautilus.logclient.EndOfLogException;
import com.emc.nautilus.logclient.LogInputStream;

import lombok.Synchronized;

public class LogInputStreamImpl extends LogInputStream {

	private final ConnectionFactory connectionFactory;
	private final String endpoint;
	Connection connection;
	long offset;
	
	Map<Long, SettableFuture<SegmentRead>> outstandingRequests = new HashMap<>();

	public LogInputStreamImpl(ConnectionFactory connectionFactory, String endpoint) {
		super();
		this.connectionFactory = connectionFactory;
		this.endpoint = endpoint;
	}

	@Override
	@Synchronized
	public void setOffset(long offset) {
		if (connection == null) {
			throw new IllegalStateException("Not connected");
		}
		RetryUtil.retryWithBackoff(new Task<ConnectionFailedException>() {
			@Override
			public void attempt() throws ConnectionFailedException {
				if (connection != null) {
					connection.drop();
					connection = connectionFactory.establishConnection(endpoint);
				}
				sendReadAt(offset);
				validateSetupReply();
			}
		});
		this.offset = offset;
	}

	private void sendReadAt(long offset) {
		// TODO Auto-generated method stub
		
	}

	private void validateSetupReply() throws ConnectionFailedException {
		// TODO Auto-generated method stub
	}

	@Override
	@Synchronized
	public long getOffset() {
		return offset;
	}

	@Override
	@Synchronized
	public void close() {
		connection.drop();
		connection = null;
	}

	@Override
	@Synchronized
	public int available() {
		if (connection == null) {
			return 0;
		} else {
			return connection.dataAvailable();
		}
	}

	@Override
	@Synchronized
	public ByteBuffer read(int length) throws EndOfLogException {
		long startOffset = offset;
		if (connection == null) {
			throw new IllegalStateException("Not connected");
		}
		ByteBuffer result = ByteBuffer.allocate(length);
		RetryUtil.retryWithBackoff(new RecoverableTask<ConnectionFailedException>() {
			@Override
			public void attempt() throws ConnectionFailedException {
				connection.read(result);
			}

			@Override
			public void recover() {
				setOffset(startOffset);
			}
		});
		return result;
	}

}
