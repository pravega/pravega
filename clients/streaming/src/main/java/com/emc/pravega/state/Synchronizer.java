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

import com.emc.pravega.stream.Stream;

/**
 * Provides a means to have state that is synchronized between many processes.
 * 
 * The pattern is to have an object of type StateT that can be updated by processing objects of
 * type UpdateT. Each host can perform logic based on its current StateT object and
 * apply updates by constructing an UpdateT to represent the update and passing it to
 * {@link #conditionallyUpdateState(Revisioned, List)}. Updates from other hosts can be obtained by calling
 * {@link #getLatestState()}
 * 
 * Updates can be conditional on the state provided being the most recent revision.
 * This provides a strong consistency with optimistic concurrency.
 * 
 * As with any optimistic concurrency system, this works best when optimism is justified:
 * i.e. Odds are another host is not updating the state at the exact same time.
 * 
 * All methods on this interface are blocking.
 * 
 * Because they are held in memory and transmitted over the network, state objects are updates
 * should be relatively compact. Implementations might explicitly enforce size limits.
 * 
 * @param <StateT> The type of the object whose updates are being synchronized.
 * @param <UpdateT> The type of updates applied to the state object.
 * @param <InitT> The type of the initializer for the state object.
 */
public interface Synchronizer<StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>> {

    /**
     * Returns the stream used by this Synchronizer.
     */
    Stream getStream();
    
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
     * Note: Even if {@link Update#applyTo(Revisioned, Revision)} updates the object in-place this
     * method may return a different instance if there have been updates that occurred that were
     * subsequently compacted away.
     * 
     * @param localState A base state which updates can be applied to to get the latest state.
     */
    StateT getLatestState(StateT localState);

    /**
     * Persists the provided update, applies it to the provided state and returns the result.
     * 
     * The update will only be applied if the localState passed
     * is up-to-date, if it is not nothing happens and null is returned instead.
     * 
     * @param localState The current revision of the state. The updates will be applied to this
     *            version and the result returned.
     * @param update The update that all other processes should receive, and that should be applied
     *            to the localState if it can be persisted.
     * @return An updated state if the update was persisted or null if localState was out of date.
     */
    StateT conditionallyUpdateState(StateT localState, UpdateT update);
    
    /**
     * Same as {@link #conditionallyUpdateState(Revisioned, Update)}, except it persists and applies
     * multiple updates at the same time. (All updates are persisted at once so they will never be
     * interleaved with other updates).
     * @param localState The current revision of the state. The updates will be applied to this
     *            version and the result returned.
     * @param update The update that all other processes should receive, and that should be applied
     *            to the localState if it can be persisted.
     * @return An updated state if the update was persisted or null if localState was out of date.
     */
    StateT conditionallyUpdateState(StateT localState, List<? extends UpdateT> update);
    
    /**
     * Persists the provided update, applies it to the provided state and returns the result.
     * 
     * If the localState is out-of-date updates will be applied first, exactly as though 
     * {@link #getLatestState()} were called first.
     * 
     * @param localState The current revision of the state. The updates will be applied to this
     *            version and the result returned.
     * @param update The update that all other processes should receive, and that should be applied
     *            to the localState once persisted.
     * @return An updated state.
     */
    StateT unconditionallyUpdateState(StateT localState, UpdateT update);
    
    /**
     * Same as {@link #unconditionallyUpdateState(Revisioned, Update)}, except it persists and applies
     * multiple updates at the same time. (All updates are persisted at once so they will never be
     * interleaved with other updates).
     * 
     * @param localState The current revision of the state. The updates will be applied to this
     *            version and the result returned.
     * @param update The updates that all other processes should receive, and that should be applied
     *            to the localState once persisted.
     * @return An updated state.
     */
    StateT unconditionallyUpdateState(StateT localState, List<? extends UpdateT> update);
    
    /**
     * This method can be used to provide an initial value for a new stream. If the stream has not
     * been previously initialized this will return the result of
     * {@link InitialUpdate#create(Revision)} If the stream was already initialized it will do
     * nothing and return the current state.
     * 
     * @param  initial The initializer for the state
     * @return The result of {@link InitialUpdate#create(Revision)} state if initialization was
     *         successful or the current state if the stream was previously initialized.
     */
    StateT initialize(InitT initial);
    
    /**
     * Provide a compaction that exactly represents the provided localState so that some of the
     * history of updates can be dropped.
     * 
     * NOTE: If compaction does not generate local state exactly corruption will occur.
     * 
     * Because no changes occur to the state, this operation does not enforce concurrency checks.
     * 
     * @param localState The localstate whose history is to be compacted away. (Not required to be
     *        current)
     * @param compaction An update that will reconstruct the localState.
     */
    void compact(StateT localState, InitT compaction);
    
}