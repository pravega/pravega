/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

/**
 * Defines various types of Producer Operations.
 */
final class ProducerOperationType extends OperationType {
    //region Stream Operations

    /**
     * An Event Operation triggered by a Producer.
     */
    static final ProducerOperationType STREAM_APPEND = new ProducerOperationType("Append");

    /**
     * A Seal Operation triggered by a Producer.
     */
    static final ProducerOperationType STREAM_SEAL = new ProducerOperationType("Seal");

    /**
     * A Create Transaction Operation triggered by a Producer.
     */
    static final ProducerOperationType CREATE_STREAM_TRANSACTION = new ProducerOperationType("Create Transaction");

    /**
     * A Merge Transaction Operation triggered by a Producer.
     */
    static final ProducerOperationType MERGE_STREAM_TRANSACTION = new ProducerOperationType("Merge Transaction");

    /**
     * A Transaction Abort Operation triggered by a Producer.
     */
    static final ProducerOperationType ABORT_STREAM_TRANSACTION = new ProducerOperationType("Abort Transaction");

    //endregion

    //region Table Operations

    /**
     * An unconditional Table Update Operation.
     */
    static final ProducerOperationType TABLE_UPDATE = new ProducerOperationType("Update");

    /**
     * A conditional Table Update Operation.
     */
    static final ProducerOperationType TABLE_UPDATE_CONDITIONAL = new ProducerOperationType("Conditional Update");

    /**
     * An unconditional Table Remove Operation.
     */
    static final ProducerOperationType TABLE_REMOVE = new ProducerOperationType("Remove");

    /**
     * A conditional Table Remove Operation.
     */
    static final ProducerOperationType TABLE_REMOVE_CONDITIONAL = new ProducerOperationType("Conditional Remove");

    //endregion

    private ProducerOperationType(String name) {
        super(name);
    }
}
