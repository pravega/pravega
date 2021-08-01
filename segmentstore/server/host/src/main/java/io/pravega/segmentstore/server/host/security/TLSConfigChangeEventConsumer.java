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
package io.pravega.segmentstore.server.host.security;

import java.nio.file.WatchEvent;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import io.netty.handler.ssl.SslContext;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link Consumer} that acts upon modification of Segment Store SSL/TLS certificate.
 *
 * It creates a new {@link SslContext} and sets it in the constructor injected {@code sslContext}.
 */
@Slf4j
public class TLSConfigChangeEventConsumer implements Consumer<WatchEvent<?>> {

    private final TLSConfigChangeHandler handler;

    public TLSConfigChangeEventConsumer(AtomicReference<SslContext> sslContext, String pathToCertificateFile,
                                 String pathToKeyFile, String[] tlsProtocolVersion) {
        handler = new TLSConfigChangeHandler(sslContext, pathToCertificateFile, pathToKeyFile, tlsProtocolVersion);
    }

    @Override
    public void accept(WatchEvent<?> watchEvent) {
        if (watchEvent != null) {
            log.info("Invoked for [{}]", watchEvent.context());
        } else {
            log.warn("Invoked for null watchEvent");
        }
        handler.handleTlsConfigChange();
    }

    @VisibleForTesting
    int getNumOfConfigChangesSinceStart() {
        return handler.getNumOfConfigChangesSinceStart();
    }
}
