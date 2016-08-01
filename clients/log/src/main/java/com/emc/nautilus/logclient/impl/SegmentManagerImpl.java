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
package com.emc.nautilus.logclient.impl;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.emc.nautilus.common.netty.ClientConnection;
import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.ConnectionFailedException;
import com.emc.nautilus.common.netty.FailingReplyProcessor;
import com.emc.nautilus.common.netty.WireCommands.CreateSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.nautilus.common.netty.WireCommands.SegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;
import com.emc.nautilus.logclient.SegmentManager;
import com.emc.nautilus.logclient.SegmentInputConfiguration;
import com.emc.nautilus.logclient.SegmentInputStream;
import com.emc.nautilus.logclient.SegmentOutputConfiguration;
import com.emc.nautilus.logclient.SegmentOutputStream;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class SegmentManagerImpl implements SegmentManager {

	private final String endpoint;
	private final ConnectionFactory connectionFactory;
	
	@Override
	@Synchronized
	public boolean createSegment(String name) {
	    CompletableFuture<Boolean> result = new CompletableFuture<>();
		ClientConnection connection = connectionFactory.establishConnection(endpoint, new FailingReplyProcessor() {
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
		try {
		    connection.send(new CreateSegment(name));
			return result.get();
		} catch (ExecutionException e) {
			throw new RuntimeException(e.getCause());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		} catch (ConnectionFailedException e) {
            throw new RuntimeException(e);
        }
	}

	@Override
	public SegmentOutputStream openSegmentForAppending(String name, SegmentOutputConfiguration config) {
	    SegmentOutputStreamImpl result = new SegmentOutputStreamImpl(connectionFactory, endpoint, UUID.randomUUID(), name);
	    try {
            result.connect();
        } catch (ConnectionFailedException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
		return result;
	}

	@Override
	public SegmentInputStream openLogForReading(String name, SegmentInputConfiguration config) {
		return new SegmentInputStreamImpl(new AsyncSegmentInputStreamImpl(connectionFactory, endpoint, name));
	}

	@Override
	public SegmentOutputStream openTransactionForAppending(String name, UUID txId) {
		throw new UnsupportedOperationException();
	}

}
