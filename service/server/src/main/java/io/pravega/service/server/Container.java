/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server;

import com.google.common.util.concurrent.Service;

/**
 * Defines a Container that can encapsulate a runnable component.
 * Has the ability to Start and Stop processing at any given time.
 */
public interface Container extends Service, AutoCloseable {
    /**
     * Gets a value indicating the Id of this container.
     */
    int getId();

    @Override
    void close();
}


