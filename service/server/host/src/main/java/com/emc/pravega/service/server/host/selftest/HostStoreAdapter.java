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

package com.emc.pravega.service.server.host.selftest;

import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Store adapter wrapping a real StreamSegmentStore and Connection Listener.
 */
public class HostStoreAdapter extends StreamSegmentStoreAdapter {
    private static final String SCOPE = "scope";
    private final int listeningPort;
    private PravegaConnectionListener listener;

    /**
     * Creates a new instance of the HostStoreAdapter class.
     *
     * @param testConfig           The TestConfig to use.
     * @param builderConfig        The ServiceBuilderConfig to use.
     * @param testCallbackExecutor An Executor to use for test-related async operations.
     */
    HostStoreAdapter(TestConfig testConfig, ServiceBuilderConfig builderConfig, Executor testCallbackExecutor) {
        super(testConfig, builderConfig, testCallbackExecutor);
        Preconditions.checkNotNull(testConfig, "testConfig");
        this.listeningPort = testConfig.getListeningPort();
    }

    //region AutoCloseable Implementation

    @Override
    public void close() {
        super.close();
        if (this.listener != null) {
            this.listener.close();
            this.listener = null;
        }
    }

    //endregion

    //region StoreAdapter Implementation

    @Override
    public CompletableFuture<Void> initialize(Duration timeout) {
        return super.initialize(timeout)
                    .thenAccept(v -> {
                        this.listener = new PravegaConnectionListener(false, this.listeningPort, getStreamSegmentStore());
                        this.listener.startListening();
                    });
    }

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, byte[] data, AppendContext context, Duration timeout) {
        return super.append(streamSegmentName, data, context, timeout);
    }

    //endregion
}
