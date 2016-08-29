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

package com.emc.pravega.service.server.reading;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a summary for a particular ReadIndex.
 */
class ReadIndexSummary {
    //region Members

    private int currentGeneration;
    private long totalSize;
    private final HashMap<Integer, Integer> generations;

    //endregion

    /**
     * Creates a new instance of the ReadIndexSummary class.
     */
    ReadIndexSummary() {
        this.currentGeneration = 0;
        this.totalSize = 0;
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
     * Records the addition of an element of the given size to the current generation.
     *
     * @param size The size of the element to add.
     * @return The value of the current generation.
     */
    synchronized int add(long size) {
        Preconditions.checkArgument(size >= 0, "size must be a non-negative number");
        this.totalSize += size;
        addToCurrentGeneration();
        return this.currentGeneration;
    }

    /**
     * Records the removal of an element of the given size from the given generation.
     *
     * @param size       The size of the element to remove.
     * @param generation The generation of the element to remove.
     */
    synchronized void remove(long size, int generation) {
        Preconditions.checkArgument(size >= 0, "size must be a non-negative number");
        this.totalSize -= size;
        if (this.totalSize < 0) {
            this.totalSize = 0;
        }

        removeFromGeneration(generation);
    }

    /**
     * Records that an element pertaining to the given generation has been used. This element will be removed from
     * its current generation and recorded in the current generation.
     *
     * @param generation The original generation of the element to touch.
     * @return The value of the current generation.
     */
    synchronized int touchOne(int generation) {
        removeFromGeneration(generation);
        addToCurrentGeneration();
        return this.currentGeneration;
    }

    /**
     * Generates a CacheManager.CacheStatus object with the information in this ReadIndexSummary object.
     *
     * @return The result
     */
    synchronized CacheManager.CacheStatus toCacheStatus() {
        AtomicInteger oldestGeneration = new AtomicInteger(Integer.MAX_VALUE);
        AtomicInteger newestGeneration = new AtomicInteger(0);
        this.generations.keySet().forEach(g -> {
            if (oldestGeneration.get() > g) {
                oldestGeneration.set(g);
            }

            if (newestGeneration.get() < g) {
                newestGeneration.set(g);
            }
        });

        return new CacheManager.CacheStatus(this.totalSize, Math.min(newestGeneration.get(), oldestGeneration.get()), newestGeneration.get());
    }

    private void addToCurrentGeneration() {
        int newCount = this.generations.getOrDefault(this.currentGeneration, 0) + 1;
        this.generations.put(this.currentGeneration, newCount);
    }

    private void removeFromGeneration(int generation) {
        int newCount = this.generations.getOrDefault(generation, 0) - 1;
        if (newCount > 0) {
            this.generations.put(generation, newCount);
        } else {
            this.generations.remove(generation);
        }
    }
}
