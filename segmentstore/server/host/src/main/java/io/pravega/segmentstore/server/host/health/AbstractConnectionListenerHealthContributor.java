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
import io.pravega.segmentstore.server.host.handler.AbstractConnectionListener;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;

import java.util.HashMap;
import java.util.Map;

public class AbstractConnectionListenerHealthContributor extends AbstractHealthContributor {
    private AbstractConnectionListener listener;
    private String host;
    private int port;

    public AbstractConnectionListenerHealthContributor(String connectionType, AbstractConnectionListener listener, String host, int port) {
        super(connectionType);
        this.listener = listener;
        this.host = host;
        this.port = port;
    }

    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) {
        Status status = Status.DOWN;
        boolean running = listener.getServerChannel().isOpen();
        if (running) {
            status = Status.NEW;
        }

        boolean ready = listener.getServerChannel().isActive();
        if (ready) {
            status = Status.UP;
        }

        Map<String, Object> details = new HashMap<String, Object>();
        details.put("host", host);
        details.put("port", port);
        builder.details(details);
        return status;
    }
}
