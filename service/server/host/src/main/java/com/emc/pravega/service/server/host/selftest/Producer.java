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

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.server.ExceptionHelpers;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents an Operation Producer for the Self Tester.
 */
public class Producer extends TestActor {
    private final String id;
    private final AtomicInteger iterationCount;
    private final AtomicBoolean canContinue;

    Producer(String id, TestConfig config, ProducerDataSource dataSource, ScheduledExecutorService executor) {
        super(config, dataSource, executor);
        this.id = id;
        this.iterationCount = new AtomicInteger();
        this.canContinue = new AtomicBoolean(true);
    }

    @Override
    protected CompletableFuture<Void> run() {
        this.canContinue.set(true);
        return FutureHelpers.loop(
                this::canLoop,
                this::runOneIteration,
                this.executorService);
    }

    private CompletableFuture<Void> runOneIteration() {
        int iterationId = iterationCount.incrementAndGet();
        return FutureHelpers
                .delayedFuture(Duration.ofMillis(new Random().nextInt(5000)), this.executorService)
                .whenComplete((r, ex) -> {
                    if (ex != null) {
                        ex = ExceptionHelpers.getRealException(ex);
                        System.out.println(String.format("Producer[%s]: Iteration %d, Failure: %s.", this.id, iterationId, ex));
                        this.canContinue.set(false);
                        throw new CompletionException(ex);
                    } else {
                        System.out.println(String.format("Producer[%s]: Iteration %d.", this.id, iterationId));
                        if (iterationId >= 5) {
                            this.canContinue.set(false);
                        }
                    }
                });
    }

    private boolean canLoop() {
        return isRunning() && this.canContinue.get();
    }

    @Override
    public String toString() {
        return String.format("Producer[%s]", this.id);
    }
}
