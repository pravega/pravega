/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.state;

import io.pravega.client.state.impl.RevisionImpl;

/**
 * A maker for a version of a {@link Revisioned} object.
 */
public interface Revision extends Comparable<Revision> {

    /**
     * Returns the actual instance. 
     * This method prevents other classes from implementing this interface.
     *
     * @return Implementation of the revision interface
     */
    RevisionImpl asImpl();
    
    /**
     * Serializes the Revision to a human readable string.
     * @return A string representation of the Revision.
     */
    @Override
    String toString();
    
    /**
     * Deserializes the revision from its serialized from obtained from calling {@link #toString()}.
     * 
     * @param revision A serialized revision.
     * @return The Revision object.
     */
    static Revision fromString(String revision) {
        return RevisionImpl.fromString(revision);
    }

}
