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
package io.pravega.client.tables.impl;

import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
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
    private final ConnectionPool connectionPool;
    @NonNull
    private final KeyValueTableClientConfiguration clientConfig;
    private final DelegationTokenProvider tokenProvider;

    @Override
    public TableSegment forSegment(@NonNull Segment segment) {
        return new TableSegmentImpl(segment, this.controller, this.connectionPool, this.clientConfig, this.tokenProvider);
    }
}
