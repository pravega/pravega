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

package io.pravega.segmentstore.server.host.health;

import com.google.common.collect.ImmutableMap;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;
import lombok.NonNull;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;

/**
 * A contributor to manage the health of zookeeper client connection.
  */
public class ZKHealthContributor extends AbstractHealthContributor {
    private final CuratorFramework zk;

    public ZKHealthContributor(@NonNull CuratorFramework zk) {
        super("zookeeper");
        this.zk = zk;
    }

    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) {
        Status status = Status.DOWN;
        boolean running = this.zk.getState() == CuratorFrameworkState.STARTED;
        if (running) {
            status = Status.NEW;
        }

        boolean ready = this.zk.getZookeeperClient().isConnected();
        if (ready) {
            status = Status.UP;
        }

        builder.details(ImmutableMap.of("zk-connection-url", this.zk.getZookeeperClient().getCurrentConnectionString()));

        return status;
    }

}
