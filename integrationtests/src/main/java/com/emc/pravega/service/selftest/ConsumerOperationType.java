/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.selftest;

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
