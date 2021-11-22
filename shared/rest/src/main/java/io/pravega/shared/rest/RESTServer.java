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
package io.pravega.shared.rest;

import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.security.JKSHelper;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.grizzly.GrizzlyFuture;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.ssl.SSLContextConfigurator;
import org.glassfish.grizzly.ssl.SSLEngineConfigurator;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

/**
 * Netty REST server implementation.
 */
@Slf4j
public class RESTServer extends AbstractIdleService {

    private final String objectId;
    private final RESTServerConfig restServerConfig;
    private final URI baseUri;
    private final ResourceConfig resourceConfig;
    private HttpServer httpServer;

    public RESTServer(RESTServerConfig restServerConfig,
                      Set<Object> resources) {
        this.objectId = "RESTServer";
        this.restServerConfig = restServerConfig;
        final String serverURI = "http://" + restServerConfig.getHost();
        this.baseUri = URI.create(serverURI + ":" + restServerConfig.getPort() + "/");

        final RESTApplication application = new RESTApplication(resources);
        this.resourceConfig = ResourceConfig.forApplication(application);
        this.resourceConfig.property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true);

        // Register the custom JSON parser.
        this.resourceConfig.register(new CustomObjectMapperProvider());

    }

    /**
     * Start REST service.
     */
    @Override
    protected void startUp() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "startUp");
        try {
            if (restServerConfig.isTlsEnabled()) {
                SSLContextConfigurator contextConfigurator = new SSLContextConfigurator();
                contextConfigurator.setKeyStoreFile(restServerConfig.getKeyFilePath());
                contextConfigurator.setKeyStorePass(JKSHelper.loadPasswordFrom(restServerConfig.getKeyFilePasswordPath()));
                httpServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, resourceConfig, true,
                        new SSLEngineConfigurator(contextConfigurator, false, false, false)
                                .setEnabledProtocols(restServerConfig.tlsProtocolVersion()));
            } else {
                httpServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, resourceConfig, true);
            }
            log.info("Started REST server listening on port: {}", this.restServerConfig.getPort());
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "startUp", traceId);
        }
    }

    /**
     * Gracefully stop REST service.
     */
    @Override
    protected void shutDown() throws Exception {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "shutDown");
        try {
            log.info("Stopping REST server listening on port: {}", this.restServerConfig.getPort());
            final GrizzlyFuture<HttpServer> shutdown = httpServer.shutdown(30, TimeUnit.SECONDS);
            log.info("Awaiting termination of REST server");
            shutdown.get();
            log.info("REST server terminated");
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "shutDown", traceId);
        }
    }
}
