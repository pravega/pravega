/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.server.host.stat;

import io.pravega.client.ClientFactory;
import io.pravega.service.contracts.StreamSegmentStore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.Data;

@Data
public class SegmentStatsFactory implements AutoCloseable {

    private static final int CORE_POOL_SIZE = 1;
    private static final int MAXIMUM_POOL_SIZE = 10;
    private static final int TASK_QUEUE_CAPACITY = 100000;
    private static final long KEEP_ALIVE_TIME_IN_SECONDS = 10L;

    private final ScheduledExecutorService maintenanceExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("cache-maintenance-%d").build());

    private final ExecutorService executor = new ThreadPoolExecutor(CORE_POOL_SIZE, // core size
            MAXIMUM_POOL_SIZE, // max size
            KEEP_ALIVE_TIME_IN_SECONDS, // idle timeout
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(TASK_QUEUE_CAPACITY),
            new ThreadFactoryBuilder().setNameFormat("stat-background-%d").build()); // queue with a size

    @VisibleForTesting
    public SegmentStatsRecorder createSegmentStatsRecorder(StreamSegmentStore store,
                                                           ClientFactory clientFactory,
                                                           AutoScalerConfig configuration) {
        AutoScaleProcessor monitor = new AutoScaleProcessor(configuration, clientFactory,
                executor, maintenanceExecutor);
        return new SegmentStatsRecorderImpl(monitor, store,
                executor, maintenanceExecutor);
    }

    public SegmentStatsRecorder createSegmentStatsRecorder(StreamSegmentStore store, AutoScalerConfig configuration) {
        AutoScaleProcessor monitor = new AutoScaleProcessor(configuration, executor, maintenanceExecutor);
        return new SegmentStatsRecorderImpl(monitor, store, executor, maintenanceExecutor);
    }

    @Override
    public void close() {
        executor.shutdown();
        maintenanceExecutor.shutdown();
    }
}
