/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
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
package com.emc.pravega.service.contracts;

import lombok.Data;

import java.util.UUID;

/**
 * Externally-specified Append Context.
 */
@Data
public class AppendContext {
    private final UUID clientId;
    private final long eventNumber;

    /**
     * Creates a new instance of the AppendContext class.
     *
     * @param clientId    The Unique Id of the client this append was received from.
     * @param eventNumber The client-assigned event number within the client context.
     */
    public AppendContext(UUID clientId, long eventNumber) {
        this.clientId = clientId;
        this.eventNumber = eventNumber;
    }

    @Override
    public String toString() {
        return String.format("ClientId = %s, eventNumber = %d", getClientId().toString(), getEventNumber());
    }
}
