/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.common.concurrent;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Helper methods for ExecutorService.
 */
public final class ExecutorServiceHelpers {
    /**
     * Gets a snapshot of the given ExecutorService.
     *
     * @param service The ExecutorService to request a snapshot on.
     * @return A Snapshot of the given ExecutorService, or null if not supported.
     */
    public static Snapshot getSnapshot(ExecutorService service) {
        Preconditions.checkNotNull(service, "service");
        if (service instanceof ThreadPoolExecutor) {
            val tpe = (ThreadPoolExecutor) service;
            return new Snapshot(tpe.getQueue().size(), tpe.getActiveCount(), tpe.getPoolSize());
        } else if (service instanceof ForkJoinPool) {
            val fjp = (ForkJoinPool) service;
            return new Snapshot(fjp.getQueuedSubmissionCount(), fjp.getActiveThreadCount(), fjp.getPoolSize());
        } else {
            return null;
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Snapshot {
        @Getter
        final int queueSize;
        @Getter
        final int activeThreadCount;
        @Getter
        final int poolSize;
    }
}
