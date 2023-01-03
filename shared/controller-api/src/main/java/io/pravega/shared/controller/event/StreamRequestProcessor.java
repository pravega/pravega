/**
 * Copyright Pravega Authors.
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
package io.pravega.shared.controller.event;

import java.util.concurrent.CompletableFuture;

public interface StreamRequestProcessor extends RequestProcessor {

    /**
     * Method to process abort txn event.
     *
     * @param abortEvent abort event
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processAbortTxnRequest(AbortEvent abortEvent);

    /**
     * Method to process commit txn event.
     *
     * @param commitEvent commit event
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processCommitTxnRequest(CommitEvent commitEvent);

    /**
     * Method to process auto scale event.
     *
     * @param autoScaleEvent auto scale event.
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processAutoScaleRequest(AutoScaleEvent autoScaleEvent);

    /**
     * Method to process scale operation event.
     *
     * @param scaleOpEvent scale operation event.
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processScaleOpRequest(ScaleOpEvent scaleOpEvent);

    /**
     * Method to process update stream event.
     *
     * @param updateStreamEvent update stream event.
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processUpdateStream(UpdateStreamEvent updateStreamEvent);

    /**
     * Method to process truncate stream event.
     *
     * @param truncateStreamEvent truncate stream event.
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processTruncateStream(TruncateStreamEvent truncateStreamEvent);

    /**
     * Method to process seal stream event.
     *
     * @param sealStreamEvent stream stream event.
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processSealStream(SealStreamEvent sealStreamEvent);

    /**
     * Method to process delete stream event.
     *
     * @param deleteStreamEvent delete stream event.
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processDeleteStream(DeleteStreamEvent deleteStreamEvent);

    /**
     * Method to process delete reader group event.
     *
     * @param createRGEvent create reader group event.
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processCreateReaderGroup(CreateReaderGroupEvent createRGEvent);

    /**
     * Method to process delete reader group event.
     *
     * @param deleteRGEvent delete reader group event.
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processDeleteReaderGroup(DeleteReaderGroupEvent deleteRGEvent);

    /**
     * Method to process update reader group event.
     *
     * @param updateRGEvent update reader group event.
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processUpdateReaderGroup(UpdateReaderGroupEvent updateRGEvent);

    /**
     * Method to process delete scope event.
     *
     * @param deleteScopeEvent update reader group event.
     * @return CompletableFuture that caller can use to synchronize.
     */
    CompletableFuture<Void> processDeleteScopeRecursive(DeleteScopeEvent deleteScopeEvent);

}
