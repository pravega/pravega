/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.fault;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface FailoverSweeper {
    /**
     * Method to check if a component is ready. Throw sweeperNotReadyException if the implementer is not ready to handle
     * failed processes. The fail over will only be attempted iff this method completes successfully. Otherwise it will be
     * retried periodically.
     * So do not block here. Return quickly with success or failure.
     *
     * @throws SweeperNotReadyException throw this exception if implementer is not ready to handle failure notifications.
     */
    void checkReady() throws SweeperNotReadyException;

    /**
     * Method to start sweeping all failed processes at the time of bootstrap. It will be called during component start,
     * and the implementer is given a supplier for list of processes. It can then compare its list of processes with the
     * supplied processes to determine failed processes and handle their fail over recovery.
     *
     * @param processes supplier to get all current processes registered and running in the cluster.
     * @return Do not block, process asynchronously and return a future
     */
    CompletableFuture<Void> sweepFailedProcesses(Supplier<Set<String>> processes);

    /**
     * Method to receive notification for failed process.
     * @param failedProcess identifier for the failed process.
     * @return Do not block, process asynchronously and return a future
     */
    CompletableFuture<Void> handleFailedProcess(String failedProcess);
}
