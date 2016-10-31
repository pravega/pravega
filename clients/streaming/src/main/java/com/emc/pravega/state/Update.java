package com.emc.pravega.state;

public interface Update<StateT extends Revisioned> {

    /**
     * Return an object of type StateT that is the same as oldState with this update applied to it.
     * Invoking {@link #getRevision()} on the result should return newRevision.
     * 
     * @param oldState the state to which should be used as the basis for the new state.
     * @param newRevision the revision for the new state.
     * @return A state that represents a state with this update applied to it. (If StateT is mutable
     *         and the update is done in-place this is the same object as oldState)
     */
    StateT applyTo(StateT oldState, Revision newRevision);

}
