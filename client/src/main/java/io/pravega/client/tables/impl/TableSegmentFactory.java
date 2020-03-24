/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.segment.impl.Segment;
import lombok.NonNull;

/**
 * Factory for {@link TableSegment} instances.
 */
interface TableSegmentFactory {
    /**
     * Creates a new {@link TableSegment} instance.
     * @param segment The {@link Segment} to create for.
     * @param tokenProvider The {@link DelegationTokenProvider} to use.
     * @return A new {@link TableSegment} instance.
     */
    TableSegment forSegment(@NonNull Segment segment, DelegationTokenProvider tokenProvider);
}
