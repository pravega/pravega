/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Defines the minimum structure of an Item that is appended to a Sequential Log.
 */
public interface LogItem {
    /**
     * Gets a value indicating the Sequence Number for this item.
     * The Sequence Number is a unique, strictly monotonically increasing number that assigns order to items.
     *
     * @return The Sequence Number.
     */
    long getSequenceNumber();

    /**
     * Serializes this item to the given OutputStream.
     *
     * @param output The OutputStream to serialize to.
     * @throws IOException           If the given OutputStream threw one.
     * @throws IllegalStateException If the serialization conditions are not met.
     */
    void serialize(OutputStream output) throws IOException;
}
