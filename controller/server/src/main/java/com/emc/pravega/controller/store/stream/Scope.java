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
package com.emc.pravega.controller.store.stream;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Properties of a Scope and operations that can be performed on it.
 * Identifier for a Scope is its name.
 */
public interface Scope {

    /**
     * Retrieve name of the scope.
     *
     * @return Name of the scope
     */
    String getName();

    /**
     * Create the scope.
     *
     * @return boolean indicating success of createScope
     */
    CompletableFuture<Boolean> createScope();

    /**
     * Delete the scope.
     *
     * @return boolean indicating success of deleteScope
     */
    CompletableFuture<Boolean> deleteScope();

    /**
     * List existing streams in scopes.
     *
     * @return List of streams in scope
     */
    CompletableFuture<List<String>> listStreamsInScope();

    /**
     * Refresh the scope object. Typically to be used to invalidate any caches.
     * This allows us reuse of scope object without having to recreate a new scope object for each new operation
     */
    void refresh();

}
