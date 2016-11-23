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
import com.emc.pravega.common.function.CallbackHelpers;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Represents an Operation for a Producer
 */
class ProducerOperation {
    //region Members

    @Getter
    private final OperationType type;
    @Getter
    private final String target;
    @Getter
    @Setter
    private Object result;
    @Setter
    private Consumer<ProducerOperation> completionCallback;
    @Setter
    private BiConsumer<ProducerOperation, Throwable> failureCallback;

    //endregion

    //region Constructor

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

    //endregion

    //region Completion

    /**
     * Indicates that this ProducerOperation completed successfully. Invokes any associated success callbacks that are
     * registered with it.
     */
    void completed() {
        Consumer<ProducerOperation> callback = this.completionCallback;
        if (callback != null) {
            CallbackHelpers.invokeSafely(callback, this, null);
        }
    }

    /**
     * Indicates that this ProducerOperation failed to complete. Invokes any associated failure callbacks that are
     * registered with it.
     */
    void failed(Throwable ex) {
        BiConsumer<ProducerOperation, Throwable> callback = this.failureCallback;
        if (callback != null) {
            CallbackHelpers.invokeSafely(callback, this, ex, null);
        }
    }

    //endregion

    @Override
    public String toString() {
        return String.format("%s: %s", this.type, this.target);
    }
}
