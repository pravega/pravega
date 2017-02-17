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
package com.emc.pravega.stream;

import com.emc.pravega.stream.impl.PositionInternal;

import java.io.Serializable;

/**
 * A position in a stream. Used to indicate where a reader died. See {@link ReaderGroup#readerOffline(String, Position)}
 * Note that this is serializable so that it can be written to an external datastore.
 *
 */
public interface Position extends Serializable {
    
    /**
     * Used internally. Do not call.
     *
     * @return implementation of position object interface
     */
    PositionInternal asImpl();
}