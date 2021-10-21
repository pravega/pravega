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
package io.pravega.client.state;

import io.pravega.client.stream.EventWriterConfig;
import java.io.Serializable;
import lombok.Builder;
import lombok.Data;

/**
 * The configuration for a Consistent replicated state synchronizer.
 */
@Data
@Builder
public class SynchronizerConfig implements Serializable {

    private static final long serialVersionUID = 2L;

    /**
     * This writer config is used by the segment writers in the StateSyncrhonizer. The default values
     * enable connection pooling and ensures the background connection retry attempts continue until the StateSyncrhonizer
     * is closed.
     */
    EventWriterConfig eventWriterConfig;
    /**
     * This size is used to allocate buffer space for the bytes the reader in the StateSyncrhonizer reads from the
     * segment. The default buffer size is 256KB.
     */
    int readBufferSize;
    
    public static class SynchronizerConfigBuilder {
        private EventWriterConfig eventWriterConfig = EventWriterConfig.builder().retryAttempts(Integer.MAX_VALUE).enableConnectionPooling(true).build();
        private int readBufferSize = 256 * 1024;
    }
}
