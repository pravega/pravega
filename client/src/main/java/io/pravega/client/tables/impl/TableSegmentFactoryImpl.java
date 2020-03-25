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

import io.pravega.client.control.impl.Controller;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.segment.impl.Segment;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Implementation for {@link TableSegmentFactory}.
 */
@RequiredArgsConstructor
class TableSegmentFactoryImpl implements TableSegmentFactory {
    @NonNull
    private final Controller controller;
    @NonNull
    private final ConnectionFactory connectionFactory;
    @NonNull
    private final DelegationTokenProvider tokenProvider;

    @Override
    public TableSegment forSegment(@NonNull Segment segment, DelegationTokenProvider tokenProvider) {
        return new TableSegmentImpl(segment.getScopedName(), this.controller, this.connectionFactory, this.tokenProvider);
    }
}
