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
package io.pravega.segmentstore.server.writer;

/**
 * Defines various states that the SegmentAggregators can be in.
 */
enum AggregatorState {
    /**
     * The SegmentAggregator is not yet initialized.
     */
    NotInitialized,

    /**
     * Normal operation mode. The SegmentAggregator is accepting new StorageOperations and flushing them normally to Storage.
     */
    Writing,

    /**
     * A disagreement has just been detected between the currently-known state of the Segment and what Storage indicates
     * and no Reconciliation steps have been taken yet to resolve it.
     */
    ReconciliationNeeded,

    /**
     * A disagreement has been detected between the currently-known state of the Segment and what Storage indicates, and
     * the SegmentAggregator is currently attempting to reconcile the two until it detects a convergence or an unrecoverable situation.
     */
    Reconciling,

    /**
     * The SegmentAggregator is closed; no further operation is allowed on it.
     */
    Closed
}
