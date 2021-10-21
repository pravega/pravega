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
package io.pravega.client.state;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
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
public interface StateSynchronizer<StateT extends Revisioned> extends AutoCloseable {
    
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
     * A function which given a state object populates a list of updates that should be applied.
     * 
     * For example:
     * <pre><code>
     * stateSynchronizer.updateState((state, updates) {@literal ->} {
     *      updates.addAll(findUpdatesForState(state));
     * });
     * </code></pre>
     * @param <StateT> The type of state it generates updates for.
     */
    @FunctionalInterface
    interface UpdateGenerator<StateT extends Revisioned>
            extends BiConsumer<StateT, List<Update<StateT>>> {
    }
    
    /**
     * Similar to {@link UpdateGenerator} but it also returns a result for the caller.
     * For example:

     * <pre><code>
     * boolean updated = stateSynchronizer.updateState((state, updates) {@literal ->} {

     *      if (!shouldUpdate(state)) {
     *          return false;
     *      }
     *      updates.addAll(findUpdatesForState(state));
     *      return true;
     * });
     * </code></pre>
     * @param <StateT> The type of state it generates updates for.
     * @param <ReturnT> The type of the result returned.
     */
    @FunctionalInterface
    interface UpdateGeneratorFunction<StateT extends Revisioned, ReturnT>
            extends BiFunction<StateT, List<Update<StateT>>, ReturnT> {
    }

    /**
     * Creates a new update for the latest state object and applies it atomically.
     * 
     * The UpdateGenerator provided will be passed the latest state object and a list which it can
     * populate with any updates that need to be applied.
     * 
     * These updates are recorded and applied conditionally on the state object that was passed to
     * the function being up to date. If another process was applying an update in parallel, the
     * state is updated and updateGenerator will be called again with the new state object so that
     * it may generate new updates. (Which may be different from the one it previously generated)
     * By re-creating the updates in this way, consistency is guaranteed. When this function returns
     * the generated updates will have been applied to the local state.
     * 
     * @param updateGenerator A function that given the current state can supply updates that should
     *            be applied.
     */
    void updateState(UpdateGenerator<StateT> updateGenerator);
    
    /**
     * Similar to {@link #updateState(UpdateGenerator)} but this version returns a result object
     * supplied by the {@link UpdateGeneratorFunction}. This is useful if the calling code wishes to
     * do something in response to the update.
     * 
     * As an example suppose the update type was MyUpdate and each update and an associated key.
     * Then it might be useful to return the updated keys:
     * <pre>
     * {@code
     * List<String> updated = stateSynchronizer.updateState((state, updates) -> {
     *      List<MyUpdate> toAdd = findUpdatesForState(state);
     *      updates.addAll(toAdd);
     *      return toAdd.stream().map(a -> a.getKey()).collect(Collectors.toList());
     * });
     * }
     * </pre>
     * @param updateGenerator A function which give the state can supply updates that should be
     *            applied.
     * @param <ReturnT> They type of the result returned by the updateGenerator
     * @return the result returned by the updateGenerator.
     */

    <ReturnT> ReturnT updateState(UpdateGeneratorFunction<StateT, ReturnT> updateGenerator);   
    
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
     * Calculates the number of bytes that have been written since the state has last been compacted by calling {@link #compact(Function)}
     * This may be useful when calculating when a compaction should occur.
     * 
     * @return The number of bytes written since the last call to {@link #compact(Function)}
     */
    long bytesWrittenSinceCompaction();
    
    /**
     * Provide a function that generates compacted version of localState so that we can drop some of the
     * history updates.
     * <p>
     * NOTE: If InitialUpdate returned does not generate local state exactly corruption will occur.
     * 
     * @param compactor An generator of InitialUpdates given a state.
     */
    void compact(Function<StateT, InitialUpdate<StateT>> compactor);
    
    
    /**
     * Closes the StateSynchronizer and frees any resources associated with it. (It may no longer be used)
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    abstract void close();
}