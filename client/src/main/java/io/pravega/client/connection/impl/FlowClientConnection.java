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
package io.pravega.client.connection.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommand;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class FlowClientConnection implements ClientConnection {

    @Getter
    private final String connectionName;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final ClientConnection channel;
    @Getter
    private final int flowId;
    private final FlowHandler handler;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public void send(WireCommand cmd) throws ConnectionFailedException {
        if (closed.get()) {
            throw new ConnectionFailedException("Connection is closed");
        }
        channel.send(cmd);
    }

    @Override
    public void send(Append append) throws ConnectionFailedException {
        if (closed.get()) {
            throw new ConnectionFailedException("Connection is closed");
        }
        channel.send(append);
    }

    @Override
    public void sendAsync(List<Append> appends, CompletedCallback callback) {
        if (closed.get()) {
            callback.complete(new ConnectionFailedException("Connection is closed"));
        } else {
            channel.sendAsync(appends, callback);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            handler.closeFlow(this);
        }
    }

    /**
     * Get the endpoint details of a segment.
     */
    @Override
    public PravegaNodeUri getLocation() {
        String[] locationInfoToken =  NameUtils.getConnectionDetails(connectionName);
        Preconditions.checkArgument(locationInfoToken.length == 2);
        return new PravegaNodeUri(locationInfoToken[0].trim(), Integer.parseInt(locationInfoToken[1].trim()));
    }

}
