package com.emc.pravega.state;

/**
 * An object that has a revision associated with it.
 * It is assumed that if two objects have the same streamName, and revision, that they are equal. IE:
 * a.equals(b) should return true.
 */
public interface Revisioned {
    
    /**
     * @return The scoped name of this stream used to persist this objcet.
     */
    String getQualifiedStreamName();
    
    /**
     * @return The revision corresponding to this object.
     */
    Revision getCurrentRevision();
}
