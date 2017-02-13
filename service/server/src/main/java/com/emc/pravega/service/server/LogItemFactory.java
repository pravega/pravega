/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.server;

import com.emc.pravega.service.server.logs.SerializationException;

import java.io.InputStream;

/**
 * Factory for LogItems.
 */
public interface LogItemFactory<T extends LogItem> {
    /**
     * Deserializes a LogItem from the given InputStream.
     *
     * @param input The InputStream to deserialize from.
     * @return The deserialized LogItem.
     * @throws SerializationException If the LogItem could not be deserialized.
     */
    T deserialize(InputStream input) throws SerializationException;
}
