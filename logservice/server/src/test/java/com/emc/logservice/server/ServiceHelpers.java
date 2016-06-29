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

package com.emc.logservice.server;

import com.emc.logservice.common.FutureHelpers;
import com.google.common.util.concurrent.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

/**
 * Helpers for com.google.common.util.concurrent.Service.
 */
public class ServiceHelpers {
    /**
     * Awaits for the given Service to shut down, whether normally or exceptionally.
     *
     * @param service The service to monitor.
     * @return A CompletableFuture that, when completed, will indicate the service is shut down. If the Service fails,
     * the Future will be completed with the failure exception.
     */
    public static CompletableFuture<Void> awaitShutDown(Service service) {
        if (service.state() == Service.State.FAILED) {
            return FutureHelpers.failedFuture(service.failureCause());
        } else if (service.state() == Service.State.TERMINATED) {
            return CompletableFuture.completedFuture(null);
        } else {
            CompletableFuture<Void> result = new CompletableFuture<>();
            service.addListener(new ServiceFailureListener(() -> result.complete(null), result::completeExceptionally), ForkJoinPool.commonPool());
            return result;
        }
    }
}
