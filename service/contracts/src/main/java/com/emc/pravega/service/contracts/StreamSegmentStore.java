/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.contracts;

import java.time.Duration;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Defines all operations that are supported on a StreamSegment.
 */
public interface StreamSegmentStore {
    /**
     * Appends a range of bytes at the end of a StreamSegment and atomically updates the given attributes. The byte range
     * will be appended as a contiguous block, however there is no guarantee of ordering between different calls to this
     * method.
     *
     * @param streamSegmentName The name of the StreamSegment to append to.
     * @param data              The data to add.
     * @param attributeUpdates  A Collection of Attribute-Values to set or update. Only the attributes contained here will
     *                          be touched; all other attributes will be left intact. May be null (which indicates no updates).
     * @param timeout           Timeout for the operation
     * @return A CompletableFuture that, will completed normally, if the add was added. If the
     * operation failed, the future will be failed with the causing exception.
     * @throws NullPointerException     If any of the arguments are null, except attributeUpdates.
     * @throws IllegalArgumentException If the StreamSegment Name is invalid (NOTE: this doesn't
     *                                  check if the StreamSegment does not exist - that exception will be set in the
     *                                  returned CompletableFuture).
     */
    CompletableFuture<Void> append(String streamSegmentName, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout);

    /**
     * Appends a range of bytes at the end of a StreamSegment an atomically updates the given attributes, but only if the
     * current length of the StreamSegment equals a certain value. The byte range will be appended as a contiguous block.
     * This method guarantees ordering (among subsequent calls).
     *
     * @param streamSegmentName The name of the StreamSegment to append to.
     * @param offset            The offset at which to append. If the current length of the StreamSegment does not equal
     *                          this value, the operation will fail with a BadOffsetException.
     * @param data              The data to add.
     * @param attributeUpdates  A Collection of Attribute-Values to set or update. Only the attributes contained here will
     *                          be touched; all other attributes will be left intact. May be null (which indicates no updates).
     * @param timeout           Timeout for the operation
     * @return A CompletableFuture that, when completed normally, will indicate the append completed successfully.
     * If the operation failed, the future will be failed with the causing exception.
     * @throws NullPointerException     If any of the arguments are null, except attributeUpdates.
     * @throws IllegalArgumentException If the StreamSegment Name is invalid (NOTE: this doesn't check if the StreamSegment
     *                                  does not exist - that exception will be set in the returned CompletableFuture).
     */
    CompletableFuture<Void> append(String streamSegmentName, long offset, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout);

    /**
     * Performs an attribute update operation on the given Segment.
     *
     * @param streamSegmentName The name of the StreamSegment which will have its attributes updated.
     * @param attributeUpdates  A Collection of Attribute-Values to set or update. Only the attributes contained here will
     *                          be touched; all other attributes will be left intact. Cannot be null.
     * @param timeout           Timeout for the operation
     * @return A CompletableFuture that, when completed normally, will indicate the update completed successfully.
     * If the operation failed, the future will be failed with the causing exception.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the StreamSegment Name is invalid (NOTE: this doesn't check if the StreamSegment
     *                                  does not exist - that exception will be set in the returned CompletableFuture).
     */
    CompletableFuture<Void> updateAttributes(String streamSegmentName, Collection<AttributeUpdate> attributeUpdates, Duration timeout);

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
     * @param waitForPendingOps If true, it waits for all operations that are currently pending to complete before returning
     *                          the result. Use this parameter if you need consistency with respect to operation order
     *                          (for example, if a series of Appends were just added but not yet processed, a call to
     *                          this method with isSync==false would not guarantee those appends are taken into consideration).
     *                          A side effect of setting this to true is that the operation may take longer to process
     *                          because it needs to wait for pending ops to complete.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the result. If the operation failed, the
     * future will be failed with the causing exception.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, boolean waitForPendingOps, Duration timeout);

    /**
     * Creates a new StreamSegment.
     *
     * @param streamSegmentName The name of the StreamSegment to create.
     * @param attributes        A Collection of Attribute-Values to set on the newly created StreamSegment.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate the operation completed. If the operation
     * failed, the future will be failed with the causing exception.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    CompletableFuture<Void> createStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout);

    /**
     * Creates a new Transaction and maps it to a Parent StreamSegment.
     *
     * @param parentStreamSegmentName The name of the Parent StreamSegment to create a transaction for.
     * @param transactionId           A unique identifier for the transaction to be created.
     * @param attributes              A Collection of Attribute-Values to set on the newly created Transaction.
     * @param timeout                 Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the name of the newly created transaction.
     * If the operation failed, the future will be failed with the causing exception.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    CompletableFuture<String> createTransaction(String parentStreamSegmentName, UUID transactionId, Collection<AttributeUpdate> attributes, Duration timeout);

    /**
     * Merges a Transaction into its parent StreamSegment.
     *
     * @param transactionName The name of the Transaction StreamSegment to merge.
     * @param timeout         Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate the operation completed. If the operation
     * failed, the future will be failed with the causing exception.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout);

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
}
