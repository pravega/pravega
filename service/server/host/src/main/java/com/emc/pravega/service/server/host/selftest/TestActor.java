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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for any component that executes as part of the Self Tester.
 */
abstract class TestActor extends AbstractService implements AutoCloseable {
    protected final TestConfig config;
    protected final ProducerDataSource dataSource;
    protected final ScheduledExecutorService executorService;
    private CompletableFuture<Void> runTask;
    private final AtomicBoolean closed;

    TestActor(TestConfig config, ProducerDataSource dataSource, ScheduledExecutorService executorService) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(dataSource, "dataSource");
        Preconditions.checkNotNull(executorService, "executorService");

        this.config = config;
        this.dataSource = dataSource;
        this.executorService = executorService;
        this.closed = new AtomicBoolean();
    }

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            stopAsync();
            ServiceShutdownListener.awaitShutdown(this, false);
            this.closed.set(true);
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        notifyStarted();
        this.runTask = run();
    }

    @Override
    protected void doStop() {
        Exceptions.checkNotClosed(this.closed.get(), this);

        this.executorService.execute(() -> {
            // Cancel the last iteration and wait for it to finish.
            if (this.runTask != null) {
                try {
                    // This doesn't actually cancel the task. We need to plumb through the code with 'checkRunning' to
                    // make sure we stop any long-running tasks.
                    this.runTask.get(this.config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
                } catch (Exception ex) {
                    notifyFailed(ex);
                    return;
                }
            }

            notifyStopped();
        });
    }


    protected abstract CompletableFuture<Void> run();
}
