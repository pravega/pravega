/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.server.logs.operations;

import io.pravega.common.util.EnumHelpers;
import io.pravega.service.server.LogItemFactory;
import io.pravega.service.server.logs.SerializationException;
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
