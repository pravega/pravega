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

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.storage.Storage;

import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class HDFSStorage implements Storage {

    private final Executor executor;
    private final HDFSStorageConfig serviceBuilderConfig;



    public HDFSStorage(HDFSStorageConfig serviceBuilderConfig, Executor executor) {
        this.serviceBuilderConfig = serviceBuilderConfig;
        this.executor  = executor;
    }

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> acquireLockForSegment(String streamSegmentName) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> releaseLockForSegment(String streamSegmentName) {
        return null;
    }

    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> concat(String targetStreamSegmentName, String sourceStreamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return null;
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return null;
    }
}
