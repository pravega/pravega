package com.emc.nautilus.logclient.impl;

import com.emc.nautilus.common.netty.ClientConnection;
import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.FailingReplyProcessor;
import com.emc.nautilus.common.netty.WireCommands.CreateSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.nautilus.common.netty.WireCommands.SegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;
import com.emc.nautilus.logclient.LogAppender;
import com.emc.nautilus.logclient.LogClient;
import com.emc.nautilus.logclient.LogInputConfiguration;
import com.emc.nautilus.logclient.LogInputStream;
import com.emc.nautilus.logclient.LogOutputConfiguration;

public class LogClientImpl implements LogClient {

	ConnectionFactory connectionFactory;
	String endpoint;
	
	@Override
	public boolean createLog(String name, long timeoutMillis) {
		ClientConnection connection = connectionFactory.establishConnection(endpoint);
		
		connection.setResponseProcessor(new FailingReplyProcessor() {
			@Override
			public void wrongHost(WrongHost wrongHost) {
				// TODO Auto-generated method stub
				
			}
			@Override
			public void segmentAlreadyExists(SegmentAlreadyExists segmentAlreadyExists) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void segmentCreated(SegmentCreated segmentCreated) {
				// TODO Auto-generated method stub
				
			}
		});
		connection.sendAsync(new CreateSegment(name));
		return false;
	}

	@Override
	public boolean logExists(String name, long timeoutMillis) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public LogAppender openLogForAppending(String name, LogOutputConfiguration config) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LogInputStream openLogForReading(String name, LogInputConfiguration config) {
		// TODO Auto-generated method stub
		return null;
	}

}
