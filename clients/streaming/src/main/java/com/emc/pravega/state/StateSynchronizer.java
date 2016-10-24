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

/**
 * Provides a means to have state that is shared between many processes.
 * 
 * The pattern is to have an object of type StateT that can be modified by processing objects of
 * type UpdateT. Each host can perform logic based on its current StateT object and if it wants to
 * perform an update it constructs an UpdateT to represent the update, passes it to
 * {@link #attemptUpdate()}. Updates from other hosts can be obtained by calling {@link #updateLocalState()}
 * 
 * Any host can update the state provided they are operating on the most recent revision.
 * This provides a strong consistency with optimistic concurrency.
 * 
 * All methods on this interface are blocking.
 * 
 * As with any optimistic concurrency system, this works best when optimism is justified:
 * IE: Odds are another host is not updating the state at the exact same time.
 * 
 * Because they are held in memory and transmitted over the network, state objects are updates
 * should be relatively compact. Implementations might explicitly enforce size limits.
 * 
 * @param <StateT> The type of the State object.
 * @param <UpdateT> The type of updates applied to the State object.
 */
public interface StateSynchronizer<StateT extends Updatable<UpdateT>, UpdateT> {

    /**
     * Get an initial state.
     * 
     * @return A base state from which updates can be tracked.
     *         NOTE: This is usually the most recent state passed to compact(). However that is not
     *         guaranteed.
     */
    StateT getInitialState();

    /**
     * Given a a local state apply all updates that have occurred since that revision if possible.
     * If the local state's revision is too old and the requested updates have been compacted away,
     * false will be returned.
     * In this case, a new state can be obtained by calling {@link #getInitialState()}.
     * 
     * @return true iff updates were applied to the provided localState
     */
    boolean synchronizeLocalState(StateT localState);

    /**
     * Attempts to persist an update, that will be applied to the localState iff the local state is
     * up to date.
     * 
     * @param localState The current revision of the state. The update will only be applied if this
     *            is in-fact the most recent revision.
     * @param update The update that all other processes should receive, and that should be applied
     *            to the localState if it can be persisted.
     * @return if the update was persisted, and subsequently applied to the provided local state.
     */
    boolean attemptUpdate(StateT localState, UpdateT update);

    /**
     * Delete all history up through the provided revision and replace it with compactState. The
     * provided state is assumed to represent the data it is compacting exactly and must not contain
     * any changes that did not come from updates.
     * 
     * Because no changes occur to the state, this operation does not enforce concurrency checks.
     * 
     * @param compactState The state that represents the state up as of the version obtained from
     *            {@link Updatable#getCurrentRevision()} (This need not be the current revision)
     */
    void compact(StateT compactState);
}