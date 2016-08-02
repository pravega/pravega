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

package com.emc.pravega.service.server;

/**
 * Defines a Metadata that can enter/exit Recovery Mode.
 */
public interface RecoverableMetadata {

    /**
     * Puts the Metadata into Recovery Mode. Recovery Mode indicates that the Metadata is about to be
     * regenerated from various sources, and is not yet ready for normal operation.
     * <p>
     * If the Metadata is in Recovery Mode, some operations may not be executed, while others are allowed to. Inspect
     * the documentation for each method to find the behavior of each.
     *
     * @throws IllegalStateException If the Metadata is already in Recovery Mode.
     */
    void enterRecoveryMode();

    /**
     * Takes the Metadata out of Recovery Mode.
     *
     * @throws IllegalStateException If the Metadata is not in Recovery Mode.
     */
    void exitRecoveryMode();

    /**
     * Resets the Metadata to its original state.
     *
     * @throws IllegalStateException If the Metadata is not in Recovery Mode.
     */
    void reset();
}
