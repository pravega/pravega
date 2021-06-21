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
package io.pravega.client.segment.impl;

import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.control.impl.Controller;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SegmentMetadataClientFactoryImpl implements SegmentMetadataClientFactory {

    private final Controller controller;
    private final ConnectionPool cp;
    
    @Override
    public SegmentMetadataClient createSegmentMetadataClient(Segment segment, DelegationTokenProvider tokenProvider) {
        return new SegmentMetadataClientImpl(segment, controller, cp, tokenProvider);
    }

}
