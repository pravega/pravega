/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.client;

/**
 * Base class for store client.
 */
public interface StoreClient extends AutoCloseable {

    Object getClient();

    StoreType getType();
}
