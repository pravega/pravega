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
package io.pravega.controller.store.task;

import com.google.common.base.Preconditions;
import lombok.Data;

/**
 * Resources managed by controller.
 * Currently there are two kinds of resources.
 * 1. Stream resource: scope/streamName
 * 2, Tx resource:     scope/streamName/txId
 */
@Data
public class Resource {
    private final String string;

    public Resource(final String... parts) {
        Preconditions.checkNotNull(parts);
        Preconditions.checkArgument(parts.length > 0);
        StringBuilder representation = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            representation.append("/");
            representation.append(parts[i]);
        }
        string = representation.toString();
    }
}
