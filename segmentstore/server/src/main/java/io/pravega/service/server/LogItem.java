/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.server;

import io.pravega.common.util.SequencedItemList;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Defines the minimum structure of an Item that is appended to a Sequential Log.
 */
public interface LogItem extends SequencedItemList.Element {
    /**
     * Serializes this item to the given OutputStream.
     *
     * @param output The OutputStream to serialize to.
     * @throws IOException           If the given OutputStream threw one.
     * @throws IllegalStateException If the serialization conditions are not met.
     */
    void serialize(OutputStream output) throws IOException;
}
