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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.function.Callbacks;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents an Operation for a Producer
 */
class ProducerOperation<T extends ProducerUpdate> {
    //region Members

    @Getter
    private final ProducerOperationType type;
    @Getter
    private final String target;
    @Getter
    @Setter
    private T update;
    @Getter
    @Setter
    private Object result;
    @Getter
    @Setter
    private int length;
    @Getter
    @Setter
    private CompletableFuture<Void> waitOn;
    @Setter
    private Consumer<ProducerOperation<T>> completionCallback;
    @Setter
    private BiConsumer<ProducerOperation<T>, Throwable> failureCallback;
    @Getter
    private long elapsedMillis = 0;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ProducerOperation class.
     *
     * @param type   The type of the operation.
     * @param target The target (Segment name) of the operation.
     */
    ProducerOperation(ProducerOperationType type, String target) {
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
     *
     * @param elapsedMillis The elapsed time, in milliseconds, for this operation.
     */
    void completed(long elapsedMillis) {
        this.elapsedMillis = elapsedMillis;
        if (this.update != null) {
            this.update.release();
        }

        Consumer<ProducerOperation<T>> callback = this.completionCallback;
        if (callback != null) {
            Callbacks.invokeSafely(callback, this, null);
        }
    }

    /**
     * Indicates that this ProducerOperation failed to complete. Invokes any associated failure callbacks that are registered
     * with it.
     */
    void failed(Throwable ex) {
        if (this.update != null) {
            this.update.release();
        }

        BiConsumer<ProducerOperation<T>, Throwable> callback = this.failureCallback;
        if (callback != null) {
            Callbacks.invokeSafely(callback, this, ex, null);
        }
    }

    //endregion

    @Override
    public String toString() {
        return String.format("%s: %s", this.type, this.target);
    }
}
