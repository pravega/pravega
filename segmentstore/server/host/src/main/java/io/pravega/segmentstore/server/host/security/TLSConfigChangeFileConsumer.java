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

import com.google.common.annotations.VisibleForTesting;
import io.netty.handler.ssl.SslContext;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Slf4j
public class TLSConfigChangeFileConsumer implements Consumer<File> {

    private final TLSConfigChangeHandler handler;

    public TLSConfigChangeFileConsumer(AtomicReference<SslContext> sslContext, String pathToCertificateFile,
                                       String pathToKeyFile, String[] tlsProtocolVersion) {
        handler = new TLSConfigChangeHandler(sslContext, pathToCertificateFile, pathToKeyFile, tlsProtocolVersion);
    }

    @Override
    public void accept(File file) {
        handler.handleTlsConfigChange();
    }

    @VisibleForTesting
    int getNumOfConfigChangesSinceStart() {
        return handler.getNumOfConfigChangesSinceStart();
    }
}
