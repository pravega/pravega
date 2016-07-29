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
package com.emc.nautilus.logclient;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

// Defines a Log output stream.
public abstract class SegmentOutputStream implements AutoCloseable {
	/**
	 * @param buff Data to be written
	 * @param onComplete future to be completed when data has been replicated and stored durrabably.
	 * @return 
	 */
	public abstract void write(ByteBuffer buff, CompletableFuture<Void> onComplete) throws SegmentSealedExcepetion;

	@Override
	public abstract void close() throws SegmentSealedExcepetion;

	public abstract void flush() throws SegmentSealedExcepetion;
}