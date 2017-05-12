/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.segmentstore.selftest;

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
