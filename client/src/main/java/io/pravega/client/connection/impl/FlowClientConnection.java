package io.pravega.client.connection.impl;

import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.WireCommand;
import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class FlowClientConnection implements ClientConnection {

    @Getter
    private final String connectionName;
    @Getter
    private final int flowId;
    private final FlowHandler handler;

    @Override
    public void send(WireCommand cmd) throws ConnectionFailedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void send(Append append) throws ConnectionFailedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void sendAsync(List<Append> appends, CompletedCallback callback) {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
