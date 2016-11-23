package com.emc.pravega.state;

public interface InitialUpdate<StateT extends Revisioned> {
    
    /**
     * Return an object of type StateT with with the provided revision.
     */
    StateT create(Revision revision);
    
}
