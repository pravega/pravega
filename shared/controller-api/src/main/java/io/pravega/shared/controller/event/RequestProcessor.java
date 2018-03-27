/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.controller.event;

import java.util.concurrent.CompletableFuture;

public interface RequestProcessor {

    /**
     * Method to process abort txn event.
     *
     * @param abortEvent abort event
     * @return
     */
    CompletableFuture<Void> processAbortTxnRequest(AbortEvent abortEvent);

    /**
     * Method to process commit txn event.
     *
     * @param commitEvent commit event
     * @return
     */
    CompletableFuture<Void> processCommitTxnRequest(CommitEvent commitEvent);

    /**
     * Method to process auto scale event.
     *
     * @param autoScaleEvent auto scale event.
     * @return
     */
    CompletableFuture<Void> processAutoScaleRequest(AutoScaleEvent autoScaleEvent);

    /**
     * Method to process scale operation event.
     *
     * @param scaleOpEvent scale operation event.
     * @return
     */
    CompletableFuture<Void> processScaleOpRequest(ScaleOpEvent scaleOpEvent);

    /**
     * Method to process update stream event.
     *
     * @param updateStreamEvent update stream event.
     * @return
     */
    CompletableFuture<Void> processUpdateStream(UpdateStreamEvent updateStreamEvent);

    /**
     * Method to process truncate stream event.
     *
     * @param truncateStreamEvent truncate stream event.
     * @return
     */
    CompletableFuture<Void> processTruncateStream(TruncateStreamEvent truncateStreamEvent);

    /**
     * Method to process seal stream event.
     *
     * @param sealStreamEvent stream stream event.
     * @return
     */
    CompletableFuture<Void> processSealStream(SealStreamEvent sealStreamEvent);

    /**
     * Method to process delete stream event.
     *
     * @param deleteStreamEvent delete stream event.
     * @return
     */
    CompletableFuture<Void> processDeleteStream(DeleteStreamEvent deleteStreamEvent);
}
