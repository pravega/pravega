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

import java.util.Map;
import java.util.NavigableMap;

/**
 * Provides a means to have state that is shared between many processes.
 * 
 * The pattern is to have an object of type StateT that can be modified by processing objects of
 * type UpdateT. Each host can perform logic based on its current StateT object and if it wants to
 * perform an update it constructs an UpdateT to represent the update, passes it to
 * {@link #attemptUpdate()} and if it is successful applies it to their own StateT object. Updates
 * from other hosts can be obtained by calling {@link #getUpdatesSince(Revision)}
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
public interface StateTracker<StateT, UpdateT> {

    /**
     * Get an initial state.
     * 
     * @return A base state from which updates can be tracked.
     *         NOTE: This is usually the most recent state passed to compact(). However that is not
     *         guaranteed.
     */
    Map.Entry<Revision, StateT> getBaseState();

    /**
     * Given a revision, return all updates that have occurred since that point.
     * If the revision provided is too old and the requested updates have been compacted away, null
     * will be returned.
     * In this case call {@link #getBaseState()} and get updates from there.
     */
    NavigableMap<Revision, UpdateT> getUpdatesSince(Revision since);

    /**
     * Provide an update to the current state.
     * 
     * @param current The current revision. The update will only be applied if this is in-fact the
     *            current revision.
     * @param update The update that all other processes should receive.
     * @return The new revision for the provided update, or null if the update failed because the
     *         provided revision was not current.
     */
    Revision attemptUpdate(Revision current, UpdateT update);

    /**
     * Delete all history up through and including the provided revision and replace it with
     * compactedState. The provided state is assumed to represent the data it is compacting exactly
     * so {@link #getUpdatesSince(Revision)} will not show any updates. So compactedState MUST NOT
     * contain any changes not represented in updates.
     * 
     * Because no changes occur to the state, this operation does not enforce any concurrency checks.
     * 
     * @param revisionToCompact Revision through which the provided state replaces. (This need not
     *            be the current revision)
     * @param compactedState The state that represents the state up through the revisionToCompact.
     */
    void compact(Revision revisionToCompact, StateT compactedState);
}