/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import java.util.function.Consumer;

/**
 * Defines a Listener for Key Updates.
 *
 * @param <KeyT>   Key Type.
 * @param <ValueT> Value Type.
 */
public interface KeyUpdateListener<KeyT, ValueT> extends AutoCloseable {
    /**
     * Gets a value indicating the Key that this Listener is attached to.
     * @return The Key.
     */
    KeyT getListeningKey();

    /**
     * This will be invoked whenever an Entry matching {@link #getListeningKey()} is inserted or updated.
     * @param value A Consumer that, when invoked, will process the given value update.
     */
    void handleValueUpdate(Consumer<GetResult<ValueT>> value);

    /**
     * This will be invoked whenever a Key matching {@link #getListeningKey()} is removed.
     * @param removeVersion A Consumer that, when invoked, will process the given key removal.
     */
    void handlevalueRemoved(Consumer<KeyVersion> removeVersion);

    /**
     * This will be invoked when the Update Listener is unregistered, which is one of the following cases:
     * * {@link TableReader#unregisterListener(KeyUpdateListener)} is invoked (if registered on a {@link TableReader}).
     * * {@link TableSegment#unregisterListener(KeyUpdateListener)} is invoked (if registered on a {@link TableSegment}).
     * * The Table Segment is sealed.
     * * The Table Segment is merged (into another one).
     * * The Table Segment is deleted.
     */
    @Override
    void close();
}
