/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.state;

/**
 * A constructor for a StateT object.
 * 
 * @param <StateT> A revisioned object that updates to are coordinated with a {@link StateSynchronizer}.
 */
public interface InitialUpdate<StateT extends Revisioned> extends Update<StateT> {
    
    /**
     * Returns an object of type StateT with the provided revision.
     * @param scopedStreamName The name of the stream that this state is associated with.
     * @param revision The revision to use
     * @return A revisioned state object
     */
    StateT create(String scopedStreamName, Revision revision);
    
    @Override
    default StateT applyTo(StateT oldState, Revision newRevision) {
        return create(oldState.getScopedStreamName(), newRevision);
    }
}
