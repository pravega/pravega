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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import com.emc.nautilus.common.netty.*;
import com.emc.nautilus.common.netty.WireCommands.*;
import com.emc.nautilus.common.utils.ReusableLatch;
import com.emc.nautilus.logclient.SegmentOutputStream;
import com.emc.nautilus.logclient.SegmentSealedExcepetion;

import io.netty.buffer.Unpooled;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class SegmentOutputStreamImpl extends SegmentOutputStream {

	private final ConnectionFactory connectionFactory;
	private final String endpoint;
	private final UUID connectionId;
	private final String segment;
	private final State state = new State();
	private final ResponseProcessor responseProcessor = new ResponseProcessor();

	private static final class State {
		private final Object lock = new Object();
		private boolean closed = false;
		private ClientConnection connection;
		private Exception exception = null;
		private final ReusableLatch connectionSetup = new ReusableLatch();
		private final ConcurrentSkipListMap<Append,CompletableFuture<Void>> inflight = new ConcurrentSkipListMap<>();
		private final ReusableLatch inflightEmpty = new ReusableLatch(true);
		private long eventNumber = 0;

		private void waitForEmptyInflight() throws InterruptedException {
			inflightEmpty.await();
		}

		private void connectionSetupComplete() {
			connectionSetup.release();
		}

		private ClientConnection getConnection() {
	          synchronized (lock) {
	              return connection;
	          }
		}
		
		private void newConnection(ClientConnection newConnection) {
			synchronized (lock) {
				connectionSetup.reset();
				exception = null;
				connection = newConnection;
			}
		}

		private void failConnection(Exception e) {
            log.warn("Connection failed due to", e);
		    ClientConnection oldConnection;
			synchronized (lock) {
				if (exception == null) {
					exception = e;
				}
				oldConnection = connection;
				connection = null;
			}
			connectionSetupComplete();
			oldConnection.close();
		}

		private ClientConnection waitForConnection() throws ConnectionFailedException, SegmentSealedExcepetion {
			try {
				connectionSetup.await();
				synchronized (lock) {
					if (exception != null) {
						throw exception;
					}
					return connection;
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			} catch (ExecutionException e) {
				throw new ConnectionFailedException(e.getCause());
			} catch (IllegalArgumentException e) {
				throw e;
			} catch (SegmentSealedExcepetion e) {
				throw e;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

        private Append createNewInflightAppend(UUID connectionId, String segment, ByteBuffer buff,
                CompletableFuture<Void> callback) {
            synchronized (lock) {
                eventNumber++;
                Append append = new Append(segment, connectionId, eventNumber, Unpooled.wrappedBuffer(buff));
                inflightEmpty.reset();
                inflight.put(append, callback);
                return append;
            }
        }

		private List<CompletableFuture<Void>> removeInflightBelow(long ackLevel) {
			synchronized (lock) {
				ArrayList<CompletableFuture<Void>> result = new ArrayList<>();
				for (Iterator<Entry<Append, CompletableFuture<Void>>> iter = inflight.entrySet().iterator(); iter.hasNext();) {
					Entry<Append, CompletableFuture<Void>> append = iter.next();
					if (append.getKey().getEventNumber() <= ackLevel) {
						result.add(append.getValue());
						iter.remove();
					} else {
						break;
					}
				}
				if (inflight.isEmpty()) {
					inflightEmpty.release();
				}
				return result;
			}
		}

		private List<Append> getAllInflight() {
			synchronized (lock) {
				return new ArrayList<>(inflight.keySet());
			}
		}

		private boolean isClosed() {
			synchronized (lock) {
				return closed;
			}
		}

		private void setClosed(boolean closed) {
			synchronized (lock) {
				this.closed = closed;
			}
		}
	}

	private final class ResponseProcessor extends FailingReplyProcessor {

		@Override
        public void wrongHost(WrongHost wrongHost) {
			state.failConnection(new ConnectionFailedException());// TODO: Probably something else.
		}

		@Override
        public void segmentIsSealed(SegmentIsSealed segmentIsSealed) {
			state.failConnection(new SegmentSealedExcepetion());
		}

		@Override
        public void noSuchSegment(NoSuchSegment noSuchSegment) {
			state.failConnection(new IllegalArgumentException(noSuchSegment.toString()));
		}

		@Override
        public void noSuchBatch(NoSuchBatch noSuchBatch) {
			state.failConnection(new IllegalArgumentException(noSuchBatch.toString()));
		}

		@Override
        public void dataAppended(DataAppended dataAppended) {
			long ackLevel = dataAppended.getEventNumber();
			ackUpTo(ackLevel);
		}

		@Override
        public void appendSetup(AppendSetup appendSetup) {
			long ackLevel = appendSetup.getLastEventNumber();
			ackUpTo(ackLevel);
			try {
                retransmitInflight();
                state.connectionSetupComplete();
            } catch (ConnectionFailedException e) {
               state.failConnection(e);
            }
		}

		private void ackUpTo(long ackLevel) {
			for (CompletableFuture<Void> toAck : state.removeInflightBelow(ackLevel)) {
				if (toAck != null) {
					toAck.complete(null);
				}
			}
		}

		private void retransmitInflight() throws ConnectionFailedException {
			for (Append append : state.getAllInflight()) {
				state.connection.send(append);
			}
		}
	}
	
	@Synchronized
	void connect() throws ConnectionFailedException {
        if (state.isClosed()) {
            throw new IllegalStateException("LogOutputStream was already closed");
        }
        if (state.getConnection() == null) {
            ClientConnection connection = connectionFactory.establishConnection(endpoint, responseProcessor);
            state.newConnection(connection);
            SetupAppend cmd = new SetupAppend(connectionId, segment);
            connection.send(cmd);
        }
	}
	
	@Override
	@Synchronized
	public void write(ByteBuffer buff, CompletableFuture<Void> callback) throws SegmentSealedExcepetion {
		ClientConnection connection = connection();
		Append append = state.createNewInflightAppend(connectionId, segment, buff, callback);
		try {
            connection.send(append);
        } catch (ConnectionFailedException e) {
            log.warn("Connection failed due to: ", e);
            connection(); //As the messages is inflight, this will perform the retransmition.
        }
	}

    private ClientConnection connection() throws SegmentSealedExcepetion {
        long delay = 1;
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                connect();
                return state.waitForConnection();
            } catch (ConnectionFailedException e) {
                state.failConnection(e);
                log.warn("Connection failed due to: ", e);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    break;
                }
                delay *= 10;
            }
        }
        throw new RuntimeException("Unable to connect to" + endpoint + ". Giving up.");
    }

	@Override
	@Synchronized
	public void close() throws SegmentSealedExcepetion {
		state.setClosed(true);
		flush();
		ClientConnection connection = state.getConnection();
		if (connection != null) {
		    connection.close();
		}
	}

	@Override
	@Synchronized
	public void flush() throws SegmentSealedExcepetion {
		try {
			ClientConnection connection = connection();
			connection.send(new KeepAlive());
			state.waitForEmptyInflight();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		} catch (ConnectionFailedException e) {
		    state.failConnection(e);
        }
	}

}
