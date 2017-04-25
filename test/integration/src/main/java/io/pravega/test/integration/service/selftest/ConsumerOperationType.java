/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.service.selftest;

/**
 * Defines various types of Producer Operations.
 */
final class ConsumerOperationType extends OperationType {
    /**
     * A Tail Read processed on the Consumer.
     */
    static final ConsumerOperationType END_TO_END = new ConsumerOperationType("End to End");

    /**
     * A Catch-up Read processed on the Consumer.
     */
    static final ConsumerOperationType CATCHUP_READ = new ConsumerOperationType("Catchup Read");

    private ConsumerOperationType(String name) {
        super(name);
    }
}
