/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.stream.impl.segment;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an OuptputStream for a segment.
 * Allows data to be appended to the end of the segment by calling {@link #write()}
 */
public abstract class SegmentOutputStream implements AutoCloseable {
    public static final int MAX_WRITE_SIZE = 1024 * 1024;

    /**
     * @param buff Data to be written. Note this is limited to {@value #MAX_WRITE_SIZE} bytes.
     * @param onComplete future to be completed when data has been replicated and stored durably.
     * @return
     */
    public abstract void write(ByteBuffer buff, CompletableFuture<Void> onComplete) throws SegmentSealedException;

    /**
     * Flushes and then closes the output stream.
     * Frees any resources associated with it.
     */
    @Override
    public abstract void close() throws SegmentSealedException;

    /**
     * Block on all writes who's future has not yet completed.
     */
    public abstract void flush() throws SegmentSealedException;
}