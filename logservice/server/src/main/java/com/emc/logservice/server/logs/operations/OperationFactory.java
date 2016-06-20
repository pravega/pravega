package com.emc.logservice.server.logs.operations;

import com.emc.logservice.contracts.StreamingException;
import com.emc.logservice.server.LogItemFactory;
import com.emc.logservice.server.logs.SerializationException;

import java.io.DataInputStream;
import java.io.InputStream;
import java.util.HashMap;

/**
 * Operation LogItem Factory.
 */
public class OperationFactory implements LogItemFactory<Operation> {
    private static final OperationConstructors constructors = new OperationConstructors();

    //region LogItemFactory Implementation

    @Override
    public Operation deserialize(InputStream input) throws SerializationException {
        DataInputStream source = new DataInputStream(input);
        Operation.OperationHeader header = new Operation.OperationHeader(source);
        return constructors.create(header, source);
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
                //TODO: there might be a better way to do this dynamically...
                map(StreamSegmentAppendOperation.OperationType, StreamSegmentAppendOperation::new);
                map(StreamSegmentSealOperation.OperationType, StreamSegmentSealOperation::new);
                map(MergeBatchOperation.OperationType, MergeBatchOperation::new);
                map(MetadataPersistedOperation.OperationType, MetadataPersistedOperation::new);
                map(StreamSegmentMapOperation.OperationType, StreamSegmentMapOperation::new);
                map(BatchMapOperation.OperationType, BatchMapOperation::new);
            }
            catch (StreamingException se) {
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
