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

package com.emc.logservice.contracts;

import java.util.UUID;

/**
 * Externally-specified Append Context.
 */
public class AppendContext {
    private final UUID clientId;
    private final long clientOffset;

    /**
     * Creates a new instance of the AppendContext class.
     *
     * @param clientId     The Unique Id of the client this append was received from.
     * @param clientOffset The append offset within the client context.
     */
    public AppendContext(UUID clientId, long clientOffset) {
        this.clientId = clientId;
        this.clientOffset = clientOffset;
    }

    /**
     * Gets a value indicating the Unique Id of the client.
     *
     * @return
     */
    public UUID getClientId() {
        return this.clientId;
    }

    /**
     * Gets a value indicating the append offset within the client context.
     *
     * @return
     */
    public long getClientOffset() {
        return this.clientOffset;
    }

    @Override
    public String toString() {
        return String.format("ClientId = %s, Offset = %d", getClientId().toString(), getClientOffset());
    }
}
