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

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main Entry Point for Pravega Segment Store Self Tester.
 */
class SelfTest extends AbstractService implements AutoCloseable {
    //region Members

    private final TestState state;
    private final TestConfig config;
    private final AtomicBoolean closed;
    private final ScheduledExecutorService executor;
    private ServiceManager actorManager;
    private final ArrayList<TestActor> actors;
    private final ProducerDataSource dataSource;

    //endregion

    //region Constructor

    SelfTest(TestConfig config) {
        Preconditions.checkNotNull(config, "config");
        this.config = config;
        this.state = new TestState();
        this.closed = new AtomicBoolean();
        this.actors = new ArrayList<>();
        this.dataSource = new ProducerDataSource(this.config, this.state);
        this.executor = Executors.newScheduledThreadPool(config.getThreadPoolSize());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            stopAsync();
            ServiceShutdownListener.awaitShutdown(this, false);
            this.executor.shutdown();
            this.closed.set(true);
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        createProducers();
        this.actorManager = new ServiceManager(this.actors);
        this.actorManager.addListener(new ServiceManager.Listener() {
            @Override
            public void healthy() {
                notifyStarted();
            }

            @Override
            public void stopped() {
                shutdownCleanup();
                notifyStopped();
            }

            @Override
            public void failure(Service service) {
                shutdownCleanup();
                notifyFailed(service.failureCause());
            }
        }, this.executor);

        this.actorManager.startAsync();
    }

    @Override
    protected void doStop() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        this.actorManager.stopAsync();
    }

    //endregion

    public CompletableFuture<Void> awaitFinished(Duration timeout) {
        return null;
    }

    private void createProducers() {
        for (int i = 0; i < this.config.getProducerCount(); i++) {
            Producer p = new Producer(Integer.toString(i), this.config, this.dataSource, this.executor);
            this.actors.add(p);
        }
    }

    private void shutdownCleanup() {
        this.actors.forEach(TestActor::close);
        this.actors.clear();
        this.actorManager = null;
    }
}
