package com.emc.pravega.state;

public interface Update<StateT extends Revisioned> {

    /**
     * Return an object of type StateT that is the same as oldState with this update applied to it.
     * Invoking {@link #getCurrentRevision()} on the result should return newRevision.
     * 
     * @param oldState the state to which should be used as the basis for the new state.
     * @param newRevision the revision for the new state.
     * @return A new state that represents a state with this update applied to it.
     */
    StateT applyTo(StateT oldState, Revision newRevision);

}
