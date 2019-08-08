package io.pravega.segmentstore.server.security;

import io.netty.handler.ssl.SslContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@RequiredArgsConstructor
@Slf4j
public class TLSConfigChangeConsumer implements Consumer<File> {

    private final TLSConfigChangeHandler handler;

    public TLSConfigChangeConsumer(AtomicReference<SslContext> sslContext, String pathToCertificateFile,
                                        String pathToKeyFile) {
        handler = new TLSConfigChangeHandler(sslContext, pathToCertificateFile, pathToKeyFile);
    }

    @Override
    public void accept(File file) {
        log.debug("Invoked for file [{}] ", file.getPath());
        handler.handleTlsConfigChange();
    }
}
