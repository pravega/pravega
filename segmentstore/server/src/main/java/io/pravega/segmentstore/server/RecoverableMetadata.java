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
package io.pravega.segmentstore.server;

/**
 * Defines the Operations on Metadata that can pertain to Recovery Mode, including taking it in/out of that mode.
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

    /**
     * Sets the current Operation Sequence Number.
     *
     * @param value The new Operation Sequence Number.
     * @throws IllegalStateException    If the Metadata is not in Recovery Mode.
     * @throws IllegalArgumentException If the new Sequence Number is not greater than the previous one.
     */
    void setOperationSequenceNumber(long value);

    /**
     * Sets the current Container Epoch.
     *
     * See getContainerEpoch() for a description of what Epoch is.
     *
     * @param value The value to set.
     * @throws IllegalStateException If the Metadata is not in Recovery Mode.
     */
    void setContainerEpoch(long value);
}
