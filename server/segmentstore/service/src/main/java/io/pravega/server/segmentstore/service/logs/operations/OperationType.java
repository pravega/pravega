/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.server.segmentstore.service.logs.operations;

import io.pravega.server.segmentstore.service.logs.SerializationException;

import java.io.DataInputStream;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Defines Types of Log Operations.
 */
@Getter(AccessLevel.PACKAGE)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum OperationType {
    Probe((byte) 0, null), // This operation cannot be serialized.
    Append((byte) 1, StreamSegmentAppendOperation::new),
    Seal((byte) 2, StreamSegmentSealOperation::new),
    Merge((byte) 3, MergeTransactionOperation::new),
    SegmentMap((byte) 4, StreamSegmentMapOperation::new),
    TransactionMap((byte) 5, TransactionMapOperation::new),
    MetadataCheckpoint((byte) 6, MetadataCheckpointOperation::new),
    UpdateAttributes((byte) 7, UpdateAttributesOperation::new);

    final byte type;
    final DeserializationConstructor deserializationConstructor;

    @FunctionalInterface
    interface DeserializationConstructor {
        Operation apply(Operation.OperationHeader header, DataInputStream source) throws SerializationException;
    }
}
