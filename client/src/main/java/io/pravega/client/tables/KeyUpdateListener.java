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

import com.google.common.annotations.Beta;
import java.util.function.Consumer;

/**
 * Defines a Listener for Key Updates.
 *
 * Notes:
 * * All registered callbacks will be invoked asynchronously on an {@link java.util.concurrent.Executor} that is either
 * provided during construction of this instance or on another {@link java.util.concurrent.Executor}. Due to the asynchronous
 * nature of such callbacks, it is not guaranteed that they will be invoked in order, so it is the responsibility of the
 * callback to verify the order (based on {@link TableEntry#getKey()#getVersion()} or {@link TableKey#getVersion()}) if needed.
 *
 * @param <KeyT>   Key Type.
 * @param <ValueT> Value Type.
 */
@Beta
public interface KeyUpdateListener<KeyT, ValueT> extends AutoCloseable {
    /**
     * Gets a pointer to the {@link KeyUpdateFilter} for this instance.
     *
     * @return The filter.
     */
    KeyUpdateFilter<KeyT> getFilter();

    /**
     * Registers a callback that will be invoked every time a Key matching {@link #getFilter()} is inserted or updated.
     * @param updateEntryCallback A {@link Consumer} that will be invoked with a {@link TableEntry} containing all
     *                            necessary information about the inserted or updated key.
     */
    void setKeyUpdatedCallback(Consumer<TableEntry<KeyT, ValueT>> updateEntryCallback);

    /**
     * Registers a callback that will be invoked every time a Key matching {@link #getFilter()} is removed.
     *
     * @param removeKeyCallback A {@link Consumer} that will be invoked with a {@link TableKey} representing the key
     *                          that was removed.
     */
    void setKeyRemovedCallback(Consumer<TableKey<KeyT>> removeKeyCallback);

    /**
     * Registers a callback that will be invoked when this {@link KeyUpdateListener} is closed.
     * @param closeCallback A {@link Consumer} that will be invoked with the appropriate {@link CloseReason}.
     */
    void setCloseCallback(Consumer<CloseReason> closeCallback);

    /**
     * Closes this instance and unregisters it.
     */
    @Override
    void close();

    /**
     * Defines a reason why a {@link KeyUpdateListener} may be closed.
     */
    enum CloseReason {
        /**
         * Table is Sealed, so no more updates should be expected.
         */
        Sealed,

        /**
         * Table is Merged (into another one), so no more updates should be expected on this one.
         */
        Merged,

        /**
         * Table is Deleted.
         */
        Deleted
    }
}
