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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents the current State of the SelfTest.
 */
class TestState {
    private final AtomicInteger generatedOperationCount;
    private final AtomicInteger successfulOperationCount;
    private final ArrayList<String> segmentNames;
    private final ArrayList<String> transactionNames;

    TestState() {
        this.generatedOperationCount = new AtomicInteger();
        this.successfulOperationCount = new AtomicInteger();
        this.segmentNames = new ArrayList<>();
        this.transactionNames = new ArrayList<>();
    }

    int newOperation() {
        return this.generatedOperationCount.incrementAndGet();
    }

    int operationCompleted() {
        return this.successfulOperationCount.incrementAndGet();
    }

    void operationFailed() {
        this.generatedOperationCount.decrementAndGet();
    }

    void recordNewSegmentName(String name) {
        synchronized (this.segmentNames) {
            this.segmentNames.add(name);
        }
    }

    void recordNewTransaction(String name) {
        recordNewSegmentName(name);
        synchronized (this.transactionNames) {
            this.transactionNames.add(name);
        }
    }

    void recordDeletedSegment(String name) {
        synchronized (this.segmentNames) {
            this.segmentNames.remove(name);
        }

        synchronized (this.transactionNames) {
            this.transactionNames.remove(name);
        }
    }

    List<String> getSegments() {
        synchronized (this.segmentNames){
            return new ArrayList<>(this.segmentNames);
        }
    }

    List<String> getTransactions() {
        synchronized (this.transactionNames){
            return new ArrayList<>(this.transactionNames);
        }
    }
}
