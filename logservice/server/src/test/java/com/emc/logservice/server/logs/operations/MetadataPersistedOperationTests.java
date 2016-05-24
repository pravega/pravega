package com.emc.logservice.server.logs.operations;

import java.util.Random;

/**
 * Unit tests for MetadataPersistedOperation class.
 */
public class MetadataPersistedOperationTests extends OperationTestsBase<MetadataPersistedOperation> {
    @Override
    protected MetadataPersistedOperation createOperation(Random random) {
        return new MetadataPersistedOperation();
    }
}
