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
package io.pravega.controller.store.stream;

import io.pravega.client.stream.StreamConfiguration;
import lombok.Data;

@Data
public class CreateStreamResponse {
    public enum CreateStatus {
        NEW,
        EXISTS_CREATING,
        EXISTS_ACTIVE,
        FAILED
    }

    private final CreateStatus status;
    private final StreamConfiguration configuration;
    private final long timestamp;
    private final int startingSegmentNumber;
}
