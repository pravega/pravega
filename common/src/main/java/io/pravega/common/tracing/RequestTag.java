/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.tracing;

import lombok.Data;

/**
 * Class to store a requestDescriptor, requestId pair in a cache (i.e., RequestTracker) for tracing purposes.
 */
@Data
public class RequestTag {

    public final static long NON_EXISTENT_ID = 0L;

    /**
     * The request descriptor is the key to access the client-generated requestId.
     */
    private final String requestDescriptor;

    /**
     * Client-generated id that will enable us to trace the end-to-end lifecycle of a request.
     */
    private final long requestId;

    public boolean isTracked() {
        return requestId != NON_EXISTENT_ID;
    }
}
