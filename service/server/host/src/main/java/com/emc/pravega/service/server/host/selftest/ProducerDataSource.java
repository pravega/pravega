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

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * Represents a Data Source for all Producers of the SelfTest.
 */
class ProducerDataSource {
    private final TestConfig config;
    private final TestState state;
    private final StoreAdapter store;
    private final HashMap<String, AppendContentGenerator> appendGenerators;
    private final Random appendSizeGenerator;

    ProducerDataSource(TestConfig config, TestState state, StoreAdapter store) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(state, "state");
        Preconditions.checkNotNull(store, "store");
        this.config = config;
        this.state = state;
        this.store = store;
        this.appendGenerators = new HashMap<>();
        this.appendSizeGenerator = new Random();
    }

    ProducerOperation nextOperation() {
        int operationIndex = getState().newOperation();
        if (operationIndex > this.config.getOperationCount()) {
            // Reached the end of the test. Nothing more to do.
            return null;
        }

        return new ProducerOperation(OperationType.Append, this.state.getSegments().get(0));
    }

    TestState getState() {
        return this.state;
    }

    byte[] generateAppendContent(String segmentName) {
        AppendContentGenerator generator = this.appendGenerators.getOrDefault(segmentName, null);
        Preconditions.checkArgument(generator != null, "No such segment was created using this DataSource");
        int maxSize = this.config.getMaxAppendSize();
        int minSize = this.config.getMinAppendSize();
        int size = maxSize;
        if (maxSize != minSize) {
            size = this.appendSizeGenerator.nextInt(maxSize - minSize) + minSize;
        }

        return generator.newAppend(size);
    }

    CompletableFuture<Void> createSegments() {
        Preconditions.checkArgument(this.state.getSegments().size() == 0, "Cannot call createSegments more than once.");
        ArrayList<CompletableFuture<Void>> segmentFutures = new ArrayList<>();

        for (int i = 0; i < this.config.getSegmentCount(); i++) {
            final int segmentId = i;
            String name = String.format("Segment_%s", segmentId);
            segmentFutures.add(
                    this.store.createStreamSegment(name, this.config.getTimeout())
                              .thenRun(() -> {
                                  this.state.recordNewSegmentName(name);
                                  this.appendGenerators.put(name, new AppendContentGenerator(segmentId));
                              }));
        }

        return FutureHelpers.allOf(segmentFutures);
    }

    CompletableFuture<Void> createTransaction(String parentSegmentName) {
        return this.store.createTransaction(parentSegmentName, this.config.getTimeout())
                         .thenAccept(transactionName -> {
                             this.state.recordNewTransaction(transactionName);
                             this.appendGenerators.put(transactionName, new AppendContentGenerator(transactionName.hashCode()));
                         });
    }

    void transactionMerged(String transactionName) {
        if (this.appendGenerators.remove(transactionName) == null) {
            throw new IllegalArgumentException("No such Transaction was created using this DataSource.");
        }

        this.state.recordDeletedSegment(transactionName);
    }

    CompletableFuture<Void> deleteAllSegments() {
        return deleteSegments(this.state.getTransactions())
                .thenCompose(v -> deleteSegments(this.state.getSegments()));
    }

    private CompletableFuture<Void> deleteSegments(Collection<String> segmentNames) {
        ArrayList<CompletableFuture<Void>> deletionFutures = new ArrayList<>();
        for (String segmentName : segmentNames) {
            deletionFutures.add(deleteSegment(segmentName));
        }

        return FutureHelpers.allOf(deletionFutures);
    }

    private CompletableFuture<Void> deleteSegment(String name) {
        return this.store.deleteStreamSegment(name, this.config.getTimeout())
                         .thenRun(() -> {
                             this.state.recordDeletedSegment(name);
                             this.appendGenerators.remove(name);
                         });
    }
}
