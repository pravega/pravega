/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server;

import com.google.common.util.concurrent.Service;

/**
 * Defines a component that pulls data from an OperationLog and writes it to a Storage. This is a background service that
 * does not expose any APIs, except for those controlling its lifecycle.
 */
public interface Writer extends Service, AutoCloseable {
    @Override
    void close();
}
