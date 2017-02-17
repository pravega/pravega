/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.logs.operations;

import com.emc.pravega.service.contracts.StreamingException;
import com.emc.pravega.service.server.LogItemFactory;
import com.emc.pravega.service.server.logs.SerializationException;

import java.io.DataInputStream;
import java.io.InputStream;
import java.util.HashMap;

/**
 * Operation LogItem Factory.
 */
public class OperationFactory implements LogItemFactory<Operation> {
    private static final OperationConstructors CONSTRUCTORS = new OperationConstructors();

    //region LogItemFactory Implementation

    @Override
    public Operation deserialize(InputStream input) throws SerializationException {
        DataInputStream source = new DataInputStream(input);
        Operation.OperationHeader header = new Operation.OperationHeader(source);
        return CONSTRUCTORS.create(header, source);
    }

    //endregion

    //region OperationConstructors

    /**
     * Helps collect and invoke constructors for Log Operations.
     */
    private static class OperationConstructors {
        private final HashMap<Byte, OperationConstructor> constructors;

        OperationConstructors() {
            constructors = new HashMap<>();
            try {
                // We purposefully do not create CachedStreamSegmentAppendOperations. Those are in-memory only and need not be serialized.
                map(StreamSegmentAppendOperation.OPERATION_TYPE, StreamSegmentAppendOperation::new);
                map(StreamSegmentSealOperation.OPERATION_TYPE, StreamSegmentSealOperation::new);
                map(MergeTransactionOperation.OPERATION_TYPE, MergeTransactionOperation::new);
                map(MetadataCheckpointOperation.OPERATION_TYPE, MetadataCheckpointOperation::new);
                map(StreamSegmentMapOperation.OPERATION_TYPE, StreamSegmentMapOperation::new);
                map(TransactionMapOperation.OPERATION_TYPE, TransactionMapOperation::new);
            } catch (StreamingException se) {
                throw new ExceptionInInitializerError(se);
            }
        }

        public void map(byte operationType, OperationConstructor constructor) throws StreamingException {
            synchronized (constructors) {
                if (constructors.containsKey(operationType)) {
                    throw new StreamingException(String.format("Duplicate Operation Type found: %d.", operationType));
                }

                constructors.put(operationType, constructor);
            }
        }

        public Operation create(Operation.OperationHeader header, DataInputStream source) throws SerializationException {
            OperationConstructor constructor = constructors.get(header.operationType);
            if (constructor == null) {
                throw new SerializationException("Operation.deserialize", String.format("Invalid Operation Type %d.", header.operationType));
            }

            return constructor.apply(header, source);
        }

        @FunctionalInterface
        private interface OperationConstructor {
            Operation apply(Operation.OperationHeader header, DataInputStream source) throws SerializationException;
        }
    }

    // endregion
}
