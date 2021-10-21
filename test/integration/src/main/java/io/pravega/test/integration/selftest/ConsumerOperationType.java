/**
 * Copyright Pravega Authors.
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
package io.pravega.test.integration.selftest;

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

    /**
     * A Table Get processed on the Consumer.
     */
    static final ConsumerOperationType TABLE_GET = new ConsumerOperationType("Table Get");

    /**
     * A Table Entry Iterator (initial call) processed on the Consumer.
     */
    static final ConsumerOperationType TABLE_ITERATOR = new ConsumerOperationType("Tbl Iterator Init.");

    /**
     * A Table Entry Iterator iteration processed on the Consumer.
     */
    static final ConsumerOperationType TABLE_ITERATOR_STEP = new ConsumerOperationType("Tbl Iterator Loop");

    private ConsumerOperationType(String name) {
        super(name);
    }
}
