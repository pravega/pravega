/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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

}
