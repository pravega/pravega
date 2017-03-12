/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega;

public interface ScopeManager extends AutoCloseable {

    /**
     * Creates a new scope.
     *
     * @param name  Name of the scope to create
     */
    void createScope(String name);

    /**
     * Deletes an existing scope. The scope must contain no
     * stream.
     *
     * @param name  Name of the scope to delete
     */
    void deleteScope(String name);
}
