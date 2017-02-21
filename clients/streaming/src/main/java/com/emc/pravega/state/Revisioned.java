/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.state;

/**
 * An object that has a revision associated with it.
 * It is assumed that if two objects have the same streamName, and revision, that they are equal. i.e.
 * a.equals(b) should return true.
 */
public interface Revisioned {
    
    /**
     * Returns the scoped name of this stream used to persist this object.
     *
     * @return String indicating full stream name including its scope appended to it
     */
    String getScopedStreamName();
    
    /**
     * Returns the revision corresponding to this object.
     *
     * @return Revision object
     */
    Revision getRevision();
}
