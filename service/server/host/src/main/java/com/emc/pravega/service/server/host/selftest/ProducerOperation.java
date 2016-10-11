/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.host.selftest;

import com.emc.pravega.common.Exceptions;
import com.google.common.base.Preconditions;

/**
 * Represents an Operation for a Producer
 */
class ProducerOperation {
    private final OperationType type;
    private final String target;

    /**
     * Creates a new instance of the ProducerOperation class.
     *
     * @param type   The type of the operation.
     * @param target The target (Segment name) of the operation.
     */
    ProducerOperation(OperationType type, String target) {
        Preconditions.checkNotNull(type, "type");
        Exceptions.checkNotNullOrEmpty(target, "target");
        this.type = type;
        this.target = target;
    }

    /**
     * Gets a value representing the type of the operation to execute.
     */
    OperationType getType() {
        return this.type;
    }

    /**
     * Gets a value representing the target (Segment name) for the operation.
     */
    String getTarget() {
        return this.target;
    }

    @Override
    public String toString() {
        return String.format("%s: %s", this.type, this.target);
    }
}
