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
package io.pravega.controller.server.health;

import com.google.common.base.Preconditions;
import io.pravega.controller.server.rpc.grpc.GRPCServer;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;

public class GRPCServerHealthContributor extends AbstractHealthContributor {

    private final GRPCServer grpcServer;

    public GRPCServerHealthContributor(String name, GRPCServer grpcServer) {
        super(name);
        this.grpcServer = Preconditions.checkNotNull(grpcServer, "grpcServer");
    }

    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) throws Exception {
        Status status = Status.DOWN;
        if (grpcServer.isRunning()) {
            status = Status.UP;
        }
        return status;
    }
}