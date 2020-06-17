/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import com.google.common.annotations.Beta;

@Beta
public interface ReaderGroupMetrics {

    /**
     * Returns the number of bytes between the last recorded position of the readers in the
     * ReaderGroup and the end of the stream(s). Note: This value may be somewhat delayed.
     *
     * @return The number of unread bytes.
     */
    long unreadBytes();
    
}
