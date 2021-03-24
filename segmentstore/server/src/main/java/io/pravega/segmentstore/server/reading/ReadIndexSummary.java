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
package io.pravega.segmentstore.server.reading;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.segmentstore.server.CacheManager;
import java.util.HashMap;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents a summary for a particular ReadIndex.
 */
@ThreadSafe
class ReadIndexSummary {
    //region Members

    @GuardedBy("this")
    private int currentGeneration;
    @GuardedBy("this")
    private final HashMap<Integer, Integer> generations;

    //endregion

    /**
     * Creates a new instance of the ReadIndexSummary class.
     */
    ReadIndexSummary() {
        this.currentGeneration = 0;
        this.generations = new HashMap<>();
    }

    //endregion

    /**
     * Updates the current generation.
     *
     * @param generation The generation to set.
     */
    synchronized void setCurrentGeneration(int generation) {
        Preconditions.checkArgument(generation >= this.currentGeneration, "New generation must be at least the value of the previous one.");
        this.currentGeneration = generation;
    }

    /**
     * Records the addition of an element to the current generation.
     *
     * @return The value of the current generation.
     */
    synchronized int addOne() {
        int newCount = this.generations.getOrDefault(this.currentGeneration, 0) + 1;
        this.generations.put(this.currentGeneration, newCount);
        return this.currentGeneration;
    }

    /**
     * Records the addition of an element to the given generation.
     *
     * @param generation The generation of the element to add.
     */
    synchronized void addOne(int generation) {
        Preconditions.checkArgument(generation >= 0, "generation must be a non-negative number");
        int newCount = this.generations.getOrDefault(generation, 0) + 1;
        this.generations.put(generation, newCount);
    }

    /**
     * Records the removal of an element from the given generation.
     *
     * @param generation The generation of the element to remove.
     */
    synchronized void removeOne(int generation) {
        int newCount = this.generations.getOrDefault(generation, 0) - 1;
        if (newCount > 0) {
            this.generations.put(generation, newCount);
        } else {
            this.generations.remove(generation);
        }
    }

    /**
     * Records that an element pertaining to the given generation has been used. This element will be removed from
     * its current generation and recorded in the current generation.
     *
     * @param generation The original generation of the element to touch.
     * @return The value of the current generation.
     */
    synchronized int touchOne(int generation) {
        if (generation == this.currentGeneration) {
            return this.currentGeneration;
        }
        removeOne(generation);
        return addOne();
    }

    /**
     * Generates a CacheManager.CacheStatus object with the information in this ReadIndexSummary object.
     */
    synchronized CacheManager.CacheStatus toCacheStatus() {
        return CacheManager.CacheStatus.fromGenerations(this.generations.keySet().iterator());
    }

    @VisibleForTesting
    synchronized int size() {
        return this.generations.values().stream().mapToInt(i -> i).sum();
    }
}
