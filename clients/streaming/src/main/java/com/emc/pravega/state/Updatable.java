package com.emc.pravega.state;

/**
 * An object that can be updated given an update of type UpdateT.
 * All implementations of this class are expected to be threadsafe.
 * All semantically significant changes to the implementation of this class are expected to occur
 * via the applyUpdate method. 
 * IE: a.equals(b) should return true if a.getCurrentRevision().equals(b.getCurrentRevision());
 */
public interface Updatable<SelfT extends Updatable<SelfT,UpdateT>, UpdateT> {

    /**
     * Apply an update.
     * After this operation is complete {@link #getCurrentRevision()} should return the newRevision.
     */
    SelfT applyUpdate(Revision newRevision, UpdateT update);

    /**
     * @return
     */
    Revision getCurrentRevision();
}
