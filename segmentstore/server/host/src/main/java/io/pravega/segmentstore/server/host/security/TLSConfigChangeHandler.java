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

import io.netty.handler.ssl.SslContext;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@RequiredArgsConstructor
@Slf4j
class TLSConfigChangeHandler {

    /**
     * A counter representing the number of times this object has been asked to
     * consume an event.
     */
    private final AtomicInteger numOfConfigChangesSinceStart = new AtomicInteger(0);

    private @NonNull final AtomicReference<SslContext> sslContext;
    private @NonNull final String pathToCertificateFile;
    private @NonNull final String pathToKeyFile;
    private @NonNull final String[] tlsProtocolVersion;

    public void handleTlsConfigChange() {
        log.info("Current reload count = {}", numOfConfigChangesSinceStart.incrementAndGet());
        sslContext.set(TLSHelper.newServerSslContext(pathToCertificateFile, pathToKeyFile, tlsProtocolVersion));
    }

    int getNumOfConfigChangesSinceStart() {
        return this.numOfConfigChangesSinceStart.get();
    }
}
