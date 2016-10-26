/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.state;

import java.util.List;

/**
 * Provides a means to have state that is synchronized between many processes.
 * 
 * The pattern is to have an object of type StateT that can be updated by processing objects of
 * type UpdateT. Each host can perform logic based on its current StateT object and
 * apply updates by constructing an UpdateT to represent the update and passing it to
 * {@link #attemptUpdate()}. Updates from other hosts can be obtained by calling
 * {@link #getLatestState()}
 * 
 * Updates can be conditional on the state provided being the most recent revision.
 * This provides a strong consistency with optimistic concurrency.
 * 
 * As with any optimistic concurrency system, this works best when optimism is justified:
 * IE: Odds are another host is not updating the state at the exact same time.
 * 
 * All methods on this interface are blocking.
 * 
 * Because they are held in memory and transmitted over the network, state objects are updates
 * should be relatively compact. Implementations might explicitly enforce size limits.
 * 
 * @param <StateT> The type of the object whose updates are being synchronized.
 * @param <UpdateT> The type of updates applied to the state object.
 */
public interface Synchronizer<StateT extends Revisioned, UpdateT extends Update<StateT>> {

    /**
     * Gets the latest version of the state object.
     */
    StateT getLatestState();

    /**
     * Gets the newest version of the state object, blocking until one newer than the one provided
     * is available.
     * This is more efficient than {@link #getLatestState()} and should be preferred if a state
     * object is available, because it only reads updates that occurred after the provided state.
     * 
     * @param localState Optional. A base state which updates can be applied to to get the latest
     *            state.
     */
    StateT getLatestState(StateT localState);

    /**
     * Persists the provided update, applies it to the provided state and returns the result.
     * 
     * If conditionalOnLastest is true the update will only be applied if the localState passed
     * is up-to-date, if it is not nothing happens and null is returned instead.
     * 
     * If conditionalOnLatest is set to false and the localState is out-of-date updates will be
     * applied first, as though {@link #getLatestState()} were called first.
     * 
     * @param localState The current revision of the state. The updates will be applied to this
     *            version and the result returned.
     * @param update The update that all other processes should receive, and that should be applied
     *            to the localState if it can be persisted.
     * @return The new state if the update was persisted or null if localState was out of date and
     *         conitionalOnLatest was true.
     */
    StateT updateState(StateT localState, UpdateT update, boolean conditionalOnLatest);
    
    /**
     * Same as {@link #updateState(Revisioned, Update, boolean)}, except it persists and applies
     * multiple updates at the same time. (All updates are persisted at once so they will never be
     * interleaved with other updates).
     */
    StateT updateState(StateT localState, List<UpdateT> update, boolean conditionalOnLatest);

    /**
     * Delete all history up through the provided revision and replace it with compactState. The
     * provided state is assumed to represent the data it is compacting exactly and must not contain
     * any changes that did not come from updates.
     * 
     * Because no changes occur to the state, this operation does not enforce concurrency checks.
     * 
     * @param compactState The state that represents the state up as of the version obtained from
     *            {@link Revisioned#getCurrentRevision()} (This need not be the current revision)
     */
    void compact(StateT compactState);
}