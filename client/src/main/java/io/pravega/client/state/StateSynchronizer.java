/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.state;

import java.util.List;
import java.util.function.Function;

/**
 * Provides a means to have state that is synchronized between many processes. This provides a
 * higher level abstraction over {@link RevisionedStreamClient}.
 * <p>
 * The pattern is to have an object of type StateT that can be updated by objects of type UpdateT.
 * Each host can perform logic based on its current StateT object and apply updates by supplying a
 * function to create UpdateT objects. Updates from other hosts can be obtained by calling
 * {@link #fetchUpdates()}
 * <p>
 * The applying of updates can be conditional on the state that was provided to their generator
 * being the most recent revision, and retrying if it is not. This provides a strong consistency
 * through optimistic concurrency.
 * <p>
 * As with any optimistic concurrency system, this works best when optimism is justified: i.e. The
 * odds are good another host is not updating the state at the exact same time.
 * <p>
 * Because they are held in memory and transmitted over the network, state objects are updates
 * should be relatively compact. Implementations might explicitly enforce size limits.
 * 
 * @param <StateT> The type of the object whose updates are being synchronized.
 */
public interface StateSynchronizer<StateT extends Revisioned> {
    
    /**
     * Gets the state object currently held in memory.
     * This is a non-blocking call.
     *
     * @return Revisioned state object
     */
    StateT getState();

    /**
     * Fetch and apply all updates needed to the state object held locally up to date.
     */
    void fetchUpdates();

    /**
     * Creates a new update for the latest state object and applies it atomically.
     * 
     * The function provided will be passed the latest state object, it returns what if any updates
     * should be applied to the state object. These updates are recorded and applied conditionally
     * on the state object that was passed to the function being up to date. If another process was
     * applying an update in parallel, the state is updated and updateGenerator will be called again
     * with the new state object so that it may generate a new update. (Which may be different from
     * the one it previously generated) By re-creating the updates in this way, consistency is
     * guaranteed. When this function returns the generated updates will have been applied to the
     * local state.
     * @param updateGenerator A function that given the current state can supply updates that should be applied.
     */
    void updateState(Function<StateT, List<? extends Update<StateT>>> updateGenerator);

    /**
     * Persists the provided update. To ensure consistent ordering of updates across hosts the
     * update is not applied locally until {@link #fetchUpdates()} is called.
     * 
     * @param update The update that all other processes should receive.
     */
    void updateStateUnconditionally(Update<StateT> update);
    
    /**
     * Same as {@link #updateStateUnconditionally(Update)}, except it persists multiple updates at
     * the same time so they will not be interleaved with other updates.
     * 
     * @param update The updates that all other processes should receive.
     */
    void updateStateUnconditionally(List<? extends Update<StateT>> update);

    /**
     * This method can be used to provide an initial value for a new stream if the stream has not
     * been previously initialized. If the stream was already initialized nothing will be changed,
     * and the local state will be updated as though {@link StateSynchronizer#fetchUpdates()}
     * 
     * @param initial The initializer for the state
     */
    void initialize(InitialUpdate<StateT> initial);

    /**
     * Provide a function that generates compacted version of localState so that we can drop some of the
     * history updates.
     * <p>
     * NOTE: If InitialUpdate returned does not generate local state exactly corruption will occur.
     * 
     * @param compactor An generator of InitialUpdates given a state.
     */
    void compact(Function<StateT, InitialUpdate<StateT>> compactor);
}