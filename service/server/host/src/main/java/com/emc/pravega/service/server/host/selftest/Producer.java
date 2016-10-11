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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Represents an Operation Producer for the Self Tester.
 */
public class Producer extends TestActor {
    private final String id;

    Producer(String id, TestConfig config, ProducerDataSource dataSource, ScheduledExecutorService executor) {
        super(config, dataSource, executor);
        this.id = id;
    }

    @Override
    protected CompletableFuture<Void> run() {
        System.out.println(String.format("Producer[%s]: Started.", this.id));
        return FutureHelpers
                .delayedFuture(Duration.ofSeconds(1), this.executorService)
                .whenComplete((r, ex)->{
                    System.out.println(String.format("Producer[%s]: Stopped.", this.id));
                });
    }

    @Override
    public String toString(){
        return String.format("Producer %s", this.id);
    }
}
