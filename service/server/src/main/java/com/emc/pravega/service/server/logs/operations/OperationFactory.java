/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.logs.operations;

import com.emc.pravega.common.util.EnumHelpers;
import com.emc.pravega.service.server.LogItemFactory;
import com.emc.pravega.service.server.logs.SerializationException;
import java.io.DataInputStream;
import java.io.InputStream;

/**
 * Operation LogItem Factory.
 */
public class OperationFactory implements LogItemFactory<Operation> {
    private static final OperationType[] MAPPING = EnumHelpers.indexById(OperationType.class, OperationType::getType);

    @Override
    public Operation deserialize(InputStream input) throws SerializationException {
        DataInputStream source = new DataInputStream(input);
        Operation.OperationHeader header = new Operation.OperationHeader(source);
        OperationType operationType = MAPPING[header.operationType];
        if (operationType == null) {
            throw new SerializationException("Operation.deserialize", String.format("Invalid Operation Type %d.", header.operationType));
        }

        return operationType.getDeserializationConstructor().apply(header, source);
    }
}
