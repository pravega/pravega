package io.pravega.segmentstore.server.security;

import com.google.common.annotations.VisibleForTesting;
import io.netty.handler.ssl.SslContext;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@RequiredArgsConstructor
@Slf4j
public class TLSConfigChangeHandler {

    /**
     * A counter representing the number of times this object has been asked to
     * consume an event.
     */
    protected final AtomicInteger numOfConfigChangesSinceStart = new AtomicInteger(0);

    protected @NonNull final AtomicReference<SslContext> sslContext;
    protected @NonNull final String pathToCertificateFile;
    protected @NonNull final String pathToKeyFile;

    public void handleTlsConfigChange() {
        log.info("Current reload count = {}", numOfConfigChangesSinceStart.incrementAndGet());
        sslContext.set(TLSHelper.newServerSslContext(pathToCertificateFile, pathToKeyFile));
    }

    @VisibleForTesting
    int getNumOfConfigChangesSinceStart() {
        return numOfConfigChangesSinceStart.get();
    }
}
