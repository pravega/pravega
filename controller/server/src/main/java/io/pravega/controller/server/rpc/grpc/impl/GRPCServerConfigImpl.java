/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.controller.server.rpc.grpc.impl;

import io.pravega.common.Exceptions;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;

import java.util.Optional;

/**
 * gRPC server config.
 */
@Data
public class GRPCServerConfigImpl implements GRPCServerConfig {
    private final int port;
    private final Optional<String> publishedRPCHost;
    private final Optional<Integer> publishedRPCPort;

    @Builder
    public GRPCServerConfigImpl(final int port, final String publishedRPCHost, final Integer publishedRPCPort) {
        Preconditions.checkArgument(port > 0, "Invalid port.");
        if (publishedRPCHost != null) {
            Exceptions.checkNotNullOrEmpty(publishedRPCHost, "publishedRPCHost");
        }
        if (publishedRPCPort != null) {
            Preconditions.checkArgument(publishedRPCPort > 0, "publishedRPCPort should be a positive integer");
        }

        this.port = port;
        this.publishedRPCHost = Optional.ofNullable(publishedRPCHost);
        this.publishedRPCPort = Optional.ofNullable(publishedRPCPort);
    }
}
