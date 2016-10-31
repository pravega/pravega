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

package com.emc.pravega.service.contracts;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Defines all operations that are supported on a StreamSegment.
 */
public interface StreamSegmentStore {
    /**
     * Appends a range of bytes at the end of a StreamSegment. The byte range will be appended as a contiguous block,
     * however there is no guarantee of ordering between different calls to this method.
     *
     * @param streamSegmentName The name of the StreamSegment to add to.
     * @param data              The data to add.
     * @param appendContext     Append context for this append.
     * @param timeout           Timeout for the operation
     * @return A CompletableFuture that, when completed normally, will contain the offset within the StreamSegment where
     * the add was added. If the operation failed, the future will be failed with the causing exception.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the StreamSegment Name is invalid (NOTE: this doesn't check if the StreamSegment
     *                                  does not exist - that exception will be set in the returned CompletableFuture).
     */
    CompletableFuture<Long> append(String streamSegmentName, byte[] data, AppendContext appendContext, Duration timeout);

    /**
     * Appends a range of bytes at the end of a StreamSegment, but only if the current length of the StreamSegment equals
     * a certain value. The byte range will be appended as a contiguous block. This method guarantees ordering (among
     * subsequent calls).
     *
     * @param streamSegmentName The name of the StreamSegment to add to.
     * @param offset            The offset at which to append. If the current length of the StreamSegment does not equal
     *                          this value, the operation will fail with a BadOffsetException.
     * @param data              The data to add.
     * @param appendContext     Append context for this append.
     * @param timeout           Timeout for the operation
     * @return A CompletableFuture that, when completed normally, will indicate the append completed successfully.
     * If the operation failed, the future will be failed with the causing exception.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the StreamSegment Name is invalid (NOTE: this doesn't check if the StreamSegment
     *                                  does not exist - that exception will be set in the returned CompletableFuture).
     */
    CompletableFuture<Void> append(String streamSegmentName, long offset, byte[] data, AppendContext appendContext, Duration timeout);

    /**
     * Initiates a Read operation on a particular StreamSegment and returns a ReadResult which can be used to consume the
     * read data.
     *
     * @param streamSegmentName The name of the StreamSegment to read from.
     * @param offset            The offset within the stream to start reading at.
     * @param maxLength         The maximum number of bytes to read.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain a ReadResult instance that can be used to
     * consume the read data. If the operation failed, the future will be failed with the causing exception.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout);

    /**
     * Gets information about a StreamSegment.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the result. IIf the operation failed, the
     * future will be failed with the causing exception.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout);

    /**
     * Creates a new StreamSegment.
     *
     * @param streamSegmentName The name of the StreamSegment to create.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate the operation completed. If the operation
     * failed, the future will be failed with the causing exception.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    CompletableFuture<Void> createStreamSegment(String streamSegmentName, Duration timeout);

    /**
     * Creates a new Transaction and maps it to a Parent StreamSegment.
     *
     * @param parentStreamSegmentName The name of the Parent StreamSegment to create a transaction for.
     * @param transactionId           A unique identifier for the transaction to be created.
     * @param timeout                 Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the name of the newly created transaction.
     * If the operation failed, the future will be failed with the causing exception.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    CompletableFuture<String> createTransaction(String parentStreamSegmentName, UUID transactionId, Duration timeout);

    /**
     * Merges a Transaction into its parent StreamSegment.
     *
     * @param transactionName The name of the Transaction StreamSegment to merge.
     * @param timeout         Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the offset within the parent StreamSegment
     * where the transaction has been merged at. If the operation failed, the future will be failed with the causing exception.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    CompletableFuture<Long> mergeTransaction(String transactionName, Duration timeout);

    /**
     * Seals a StreamSegment for modifications.
     *
     * @param streamSegmentName The name of the StreamSegment to seal.
     * @param timeout           Timeout for the operation
     * @return A CompletableFuture that, when completed normally, will contain the final length of the StreamSegment.
     * If the operation failed, the future will be failed with the causing exception.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout);

    /**
     * Deletes a StreamSegment.
     *
     * @param streamSegmentName The name of the StreamSegment to delete.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate the operation completed. If the operation
     * failed, the future will be failed with the causing exception.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout);

    /**
     * Gets the Append Context for the last received append. This includes all appends made with the given client id,
     * regardless of whether they were committed or are still in flight. If the last append for this StreamSegment/ClientId
     * is still in flight, this method will wait until it is processed (or failed) and return the appropriate result/code.
     *
     * @param streamSegmentName The name of the StreamSegment to inquire about.
     * @param clientId          A UUID representing the Client Id to inquire about.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the requested information. If any exception
     * occurred during processing, or if the last append in flight failed to process, the future will be failed with the
     * causing exception.. The future will also fail with a StreamSegmentNotExistsException if the given StreamSegmentName
     * does not exist.
     */
    CompletableFuture<AppendContext> getLastAppendContext(String streamSegmentName, UUID clientId, Duration timeout);
}
