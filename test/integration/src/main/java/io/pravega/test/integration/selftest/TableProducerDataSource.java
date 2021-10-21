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
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BlockingDrainingQueue;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.test.integration.selftest.adapters.StoreAdapter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.GuardedBy;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

class TableProducerDataSource extends ProducerDataSource<TableUpdate> {
    //region Members

    private final ConcurrentHashMap<String, UpdateGenerator> updateGenerators;
    private final BlockingDrainingQueue<UpdatedKey> recentUpdates;
    private final AtomicInteger recentUpdatesAddCount;
    @GuardedBy("random")
    private final Random random;

    //endregion

    //region Constructor

    TableProducerDataSource(TestConfig config, TestState state, StoreAdapter store) {
        super(config, state, store);
        Preconditions.checkArgument(store.isFeatureSupported(StoreAdapter.Feature.Tables), "StoreAdapter must support Tables.");
        this.updateGenerators = new ConcurrentHashMap<>();
        this.recentUpdates = new BlockingDrainingQueue<>();
        this.recentUpdatesAddCount = new AtomicInteger();
        this.random = new Random();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.recentUpdates.close();
    }

    //endregion

    //region ProducerDataSource Implementation

    @Override
    ProducerOperation<TableUpdate> nextOperation(int producerId) {
        int operationIndex = this.state.newOperation();
        if (operationIndex > this.config.getOperationCount()) {
            // End of the test.
            return null;
        }

        if (this.state.isWarmup() && operationIndex == this.config.getWarmupCount()) {
            this.state.setWarmup(false);
        }

        // Decide whether to remove.
        boolean removal = decide(this.config.getTableRemovePercentage());

        // Choose operation type based on Test Configuration. For simplicity we don't mix conditional and unconditional
        // updates in this type of test.
        val opType = removal ? ProducerOperationType.TABLE_REMOVE : ProducerOperationType.TABLE_UPDATE;

        // Choose a Table to update.
        val si = this.state.getStreamOrTransaction(operationIndex);
        si.operationStarted();
        ProducerOperation<TableUpdate> result = new ProducerOperation<>(opType, si.getName());

        // Generate the update and link callbacks.
        TableUpdate update = generateUpdate(result.getTarget(), removal);
        result.setUpdate(update);
        result.setLength(update.getKey().getLength() + (update.isRemoval() ? 0 : update.getValue().getLength()));
        result.setCompletionCallback(this::operationCompletionCallback);
        result.setFailureCallback(this::operationFailureCallback);
        return result;
    }

    @Override
    CompletableFuture<Void> createAll() {
        Preconditions.checkArgument(this.state.getAllStreams().size() == 0, "Cannot call createAll more than once.");
        ArrayList<CompletableFuture<Void>> creationFutures = new ArrayList<>();

        TestLogger.log(LOG_ID, "Creating Tables.");
        for (int i = 0; i < this.config.getStreamCount(); i++) {
            // Table Names are of the form: Table<TestId><TableId> - to avoid clashes between different tests.
            String name = String.format("Table%s%s", this.config.getTestId(), i);
            creationFutures.add(this.store.createTable(name, this.config.getTimeout())
                    .thenRun(() -> {
                        this.state.recordNewStreamName(name);
                        this.updateGenerators.put(name, new UpdateGenerator());
                    }));
        }

        return Futures.allOf(creationFutures);
    }

    @Override
    CompletableFuture<Void> deleteAll() {
        TestLogger.log(LOG_ID, "Deleting Tables.");
        ArrayList<CompletableFuture<Void>> deletionFutures = new ArrayList<>();
        for (String segmentName : this.state.getAllStreamNames()) {
            try {
                deletionFutures.add(deleteStream(segmentName));
            } catch (Throwable ex) {
                if (Exceptions.mustRethrow(ex) || !(Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException)) {
                    throw ex;
                }
            }
        }

        return Futures.allOf(deletionFutures);
    }

    //endregion

    //region TableProducerDataSource Implementation

    /**
     * Fetches the recent {@link UpdatedKey}s that were updated or removed.
     *
     * @param maxCount Maximum result size.
     * @return A CompletableFuture that, when completed, will contain at least one result. If there are no recent updates
     * this future will not be completed immediately; it will be completed when the next result is generated.
     */
    CompletableFuture<Queue<UpdatedKey>> fetchRecentUpdates(int maxCount) {
        if (this.recentUpdates.size() == 0 && this.recentUpdatesAddCount.get() >= this.config.getOperationCount()) {
            // End of the test.
            return CompletableFuture.completedFuture(null);
        }

        return this.recentUpdates.take(maxCount);
    }

    private CompletableFuture<Void> deleteStream(String name) {
        return Futures.exceptionallyExpecting(this.store.deleteTable(name, this.config.getTimeout()),
                ex -> ex instanceof StreamSegmentNotExistsException,
                null)
                      .thenRun(() -> postStreamDeletion(name));
    }

    private void postStreamDeletion(String name) {
        this.updateGenerators.remove(name);
        this.state.recordDeletedStream(name);
    }

    private TableUpdate generateUpdate(String target, boolean removal) {
        UpdateGenerator generator = this.updateGenerators.get(target);
        if (generator == null) {
            // If the argument is indeed correct, this segment was deleted between the time the operation got generated
            // and when this method was invoked.
            throw new UnknownTargetException(target);
        }

        return removal ? generator.generateRemoval() : generator.generateUpdate();
    }

    private void operationCompletionCallback(ProducerOperation<TableUpdate> op) {
        this.state.operationCompleted(op.getTarget());
        this.state.recordDuration(op.getType(), op.getElapsedMillis());
        this.state.recordDataWritten(op.getLength());
        this.state.recordTableModification(1);
        UpdateGenerator generator = this.updateGenerators.get(op.getTarget());
        if (generator != null && !op.getUpdate().isRemoval() && op.getResult() != null) {
            // Update the generator's knowledge of this key's version so it may reuse that for subsequent operations.
            generator.recordUpdate(op.getUpdate().getKeyId(), (Long) op.getResult());
        }

        this.recentUpdatesAddCount.incrementAndGet();
        if (!op.getUpdate().isRemoval()) {
            this.recentUpdates.add(new UpdatedKey(op.getTarget(), op.getUpdate().getKeyId()));
        }
    }

    private void operationFailureCallback(ProducerOperation<?> op, Throwable ex) {
        this.state.operationFailed(op.getTarget());
    }

    private boolean decide(int probabilityPercentage) {
        synchronized (this.random) {
            return this.random.nextInt(101) <= probabilityPercentage;
        }
    }

    //endregion

    //region UpdateGenerator

    private class UpdateGenerator {
        @GuardedBy("this")
        private final ArrayDeque<KeyWithVersion> recentKeys; // Keys to Versions.

        UpdateGenerator() {
            this.recentKeys = new ArrayDeque<>();
        }

        synchronized void recordUpdate(UUID keyId, long version) {
            this.recentKeys.addLast(new KeyWithVersion(keyId, version));
        }

        TableUpdate generateRemoval() {
            KeyWithVersion toUpdate = pickKey();
            return TableUpdate.removal(toUpdate.keyId, config.getTableKeyLength(), toUpdate.version);
        }

        TableUpdate generateUpdate() {
            KeyWithVersion toUpdate = pickKey();
            return TableUpdate.update(toUpdate.keyId, config.getTableKeyLength(), config.getMaxAppendSize(), toUpdate.version);
        }

        synchronized private KeyWithVersion pickKey() {
            // Choose whether to pick existing key or not (using configuration).
            if (decide(config.getTableNewKeyPercentage()) || this.recentKeys.isEmpty()) {
                return new KeyWithVersion(UUID.randomUUID(), config.isTableConditionalUpdates() ? TableKey.NOT_EXISTS : null);
            } else {
                val first = this.recentKeys.removeFirst();
                return new KeyWithVersion(first.keyId, config.isTableConditionalUpdates() ? first.version : null);
            }
        }
    }

    //endregion

    //region KeyWithVersion

    @RequiredArgsConstructor
    private static class KeyWithVersion {
        final UUID keyId;
        final Long version;

        @Override
        public String toString() {
            return String.format("KeyId = %s, Version = %s", this.keyId, this.version);
        }
    }

    //endregion

    //region UpdateKey

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    static class UpdatedKey {
        private final String target;
        private final UUID keyId;

        @Override
        public String toString() {
            return String.format("%s: %s", this.target, this.keyId);
        }
    }

    //endregion
}
