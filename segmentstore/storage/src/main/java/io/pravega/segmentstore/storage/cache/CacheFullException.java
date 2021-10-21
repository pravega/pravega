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
package io.pravega.segmentstore.storage.cache;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.segmentstore.storage.CacheException;

/**
 * {@link CacheException} thrown whenever the {@link CacheStorage} is full and cannot accept any extra data.
 */
public class CacheFullException extends CacheException {
    @VisibleForTesting
    public CacheFullException(String message) {
        super(message);
    }
}
