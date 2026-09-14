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
package io.pravega.storage.filesystem;

import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;

public class FileSystemStorageConfigTest {
    @Test
    public void testChannelCacheExpiration() {
        FileSystemStorageConfig defaultConfig = FileSystemStorageConfig.builder().build();
        assertEquals(Duration.ofSeconds(600), defaultConfig.getChannelCacheExpiration());
        assertEquals(1024, defaultConfig.getReadChannelCacheSize());
        assertEquals(1024, defaultConfig.getWriteChannelCacheSize());

        FileSystemStorageConfig customConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.CHANNEL_CACHE_EXPIRATION, 42)
                .build();
        assertEquals(Duration.ofSeconds(42), customConfig.getChannelCacheExpiration());
    }
}
