/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.connection.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SocketConnectionFactoryImpl implements ConnectionFactory {

    private static final AtomicInteger POOLCOUNT = new AtomicInteger();
    
    private final AtomicInteger openSocketCount = new AtomicInteger();

    private final ClientConfig clientConfig;
    @Getter
    private final ScheduledExecutorService internalExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public SocketConnectionFactoryImpl(ClientConfig clientConfig) {
        this(clientConfig, (Integer) null);
    }

    @VisibleForTesting
    public SocketConnectionFactoryImpl(ClientConfig clientConfig, Integer numThreadsInPool) {
        this.clientConfig = Preconditions.checkNotNull(clientConfig, "clientConfig");
        this.internalExecutor = ExecutorServiceHelpers.newScheduledThreadPool(getThreadPoolSize(numThreadsInPool),
                "clientInternal-" + POOLCOUNT.incrementAndGet());
    }

    @VisibleForTesting
    public SocketConnectionFactoryImpl(ClientConfig clientConfig, ScheduledExecutorService executor) {
        this.clientConfig = Preconditions.checkNotNull(clientConfig, "clientConfig");
        this.internalExecutor = executor;
    }


    @Override
    public CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri endpoint, ReplyProcessor rp) {
        openSocketCount.incrementAndGet();
        return TcpClientConnection
            .connect(endpoint, clientConfig, rp, internalExecutor, openSocketCount::decrementAndGet)
            .thenApply(c -> c);
    }

    private int getThreadPoolSize(Integer threadCount) {
        if (threadCount != null) {
            return threadCount;
        }
        String configuredThreads = System.getProperty("pravega.client.internal.threadpool.size", null);
        if (configuredThreads != null) {
            return Integer.parseInt(configuredThreads);
        }
        return Math.max(2, Runtime.getRuntime().availableProcessors());
    }

    @Override
    public void close() {
        log.info("Shutting down connection factory");
        if (closed.compareAndSet(false, true)) {
            ExecutorServiceHelpers.shutdown(internalExecutor);
        }
    }

    @VisibleForTesting
    public int getOpenSocketCount() {
        return openSocketCount.get();
    }
}
