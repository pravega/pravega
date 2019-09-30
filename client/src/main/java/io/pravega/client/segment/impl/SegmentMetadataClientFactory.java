/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.client.security.auth.DelegationTokenProvider;

/**
 * Creates {@link SegmentMetadataClient} for metadata operations on existing segments.
 */
public interface SegmentMetadataClientFactory {
    /**
     * Opens an existing segment for metadata operations. This operation will fail if the
     * segment does not exist.
     * This operation may be called multiple times on the same segment from the
     * same client (i.e., there can be concurrent metadata clients in the same
     * process space).
     *
     * @param segment The segment to create a metadata client for.
     * @param tokenProvider The {@link DelegationTokenProvider} instance to be used for obtaining a delegation token.
     * @return New instance of SegmentMetadataClient for the provided segment.
     */
    SegmentMetadataClient createSegmentMetadataClient(Segment segment, DelegationTokenProvider tokenProvider);

}
