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
package io.pravega.segmentstore.server.logs;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.util.BlockingDrainingQueue;
import io.pravega.segmentstore.server.logs.health.InMemoryLogHealthContributor;
import io.pravega.segmentstore.server.logs.operations.Operation;
import javax.annotation.concurrent.GuardedBy;

import io.pravega.shared.health.HealthConnector;
import io.pravega.shared.health.HealthContributor;
import lombok.AccessLevel;
import lombok.Getter;

import java.util.Queue;

/**
 * {@link BlockingDrainingQueue} implementation for {@link Operation}s. Prevents adding {@link Operation}s out of order.
 */
public class InMemoryLog extends BlockingDrainingQueue<Operation> implements HealthConnector {
    /**
     * The last sequence number added. This field is only accessed in {@link #addInternal}, which is guaranteed to be
     * executed while holding the base class' lock, hence no need for extra synchronization here.
     */
    @GuardedBy("AbstractDrainingQueue.this.lock")
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private long lastSequenceNumber = Operation.NO_SEQUENCE_NUMBER;

    @Getter(AccessLevel.PRIVATE)
    private final HealthContributor contributor;

    public InMemoryLog() {
        this.contributor = new InMemoryLogHealthContributor("InMemoryLog", this);
    }

    /**
     * See {@link BlockingDrainingQueue#addInternal}.
     * NOTE: this method is invoked while holding the super class' lock; as such, no further synchronization is needed here.
     *
     * @param item The item to include.
     * @throws OutOfOrderOperationException If item's Sequence Number is out of order.
     */
    @Override
    @GuardedBy("AbstractDrainingQueue.this.lock")
    protected void addInternal(Operation item) {
        if (this.lastSequenceNumber >= item.getSequenceNumber()) {
            throw new OutOfOrderOperationException(String.format("Operation '%s' is out of order. Expected sequence number of at least %s.",
                    item, this.lastSequenceNumber));
        }
        super.addInternal(item);
        this.lastSequenceNumber = item.getSequenceNumber();
    }

    @Override
    public Queue<Operation> close() {
        contributor.close();
        return super.close();
    }

    public static class OutOfOrderOperationException extends IllegalStateException {
        public OutOfOrderOperationException(String message) {
            super(message);
        }
    }
}
