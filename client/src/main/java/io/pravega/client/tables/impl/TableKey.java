/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

/**
 * A Table Key with a Version.
 *
 * @param <KeyT> Type of the Key.
 */
public interface TableKey<KeyT> {
    /**
     * The Key.
     * @return key.
     */
    KeyT getKey();

    /**
     * The Version. If null, any updates for this Key will be unconditional.
     * @return {@link KeyVersion}.
     */
    KeyVersion getVersion();
}
