/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.cache;

import io.pravega.segmentstore.storage.CacheException;

/**
 * {@link CacheException} thrown whenever the {@link CacheStorage} is full and cannot accept any extra data.
 */
public class CacheFullException extends CacheException {
    CacheFullException(String message) {
        super(message);
    }
}
