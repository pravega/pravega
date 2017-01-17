/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.server.actor;

import com.emc.pravega.common.cluster.ClusterListener;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.controller.actor.ActorSystem;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ActorSystemClusterListener implements ClusterListener {

    private final ActorSystem actorSystem;

    ActorSystemClusterListener(ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }

    @Override
    public void onEvent(EventType type, Host host) {
        log.debug(String.format("Cluster event %s received for host %s", type, host));
        if (type == EventType.HOST_REMOVED) {
            actorSystem.notifyHostFailure(host);
        }
    }
}
