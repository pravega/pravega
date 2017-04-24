/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.selftest;

/**
 * Defines various types of Producer Operations.
 */
final class ProducerOperationType extends OperationType {
    /**
     * An Append Operation triggered by a Producer.
     */
    static final ProducerOperationType APPEND = new ProducerOperationType("Append");

    /**
     * A Seal Operation triggered by a Producer.
     */
    static final ProducerOperationType SEAL = new ProducerOperationType("Seal");

    /**
     * A Create Transaction Operation triggered by a Producer.
     */
    static final ProducerOperationType CREATE_TRANSACTION = new ProducerOperationType("Create Transaction");

    /**
     * A Merge Transaction Operation triggered by a Producer.
     */
    static final ProducerOperationType MERGE_TRANSACTION = new ProducerOperationType("Merge Transaction");

    private ProducerOperationType(String name) {
        super(name);
    }
}
