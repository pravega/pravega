package io.pravega.client.control.impl;

import io.pravega.common.Timer;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.CompletableFuture;

/**
 * This is used for caching the segment endpoint information,
 * to avoid triggering duplicate calls in order to fetch the endpoint details from network/server.
 */
@Data
@RequiredArgsConstructor
public class CachedPravegaNodeUri {

    public static final int maxBackoffMillis = 20000; // TODO: to be moved to a common place, refer the same in EventWriter as well

    @NonNull
    private final Timer timer;

    @NonNull
    private CompletableFuture<PravegaNodeUri> pravegaNodeUri;

}
