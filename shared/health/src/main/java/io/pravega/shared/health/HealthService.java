/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

import java.io.IOException;
import java.net.URI;

/**
 * The top level interface used to provide any and all health related information for a particular component
 * of Pravega. It holds the {@link ContributorRegistry} and provides the endpoint used to make health information
 * accessible to clients.
 *
 * A {@link HealthService} should provide four endpoints:
 *  * /health               A route providing the aggregate of the three routes listed below.
 *  * /health/readiness     Exposes the top level 'ready' status.
 *  * /health/liveness      Exposes the top level 'liveness' status.
 *  * /health/details       Exposes the aggregate {@link Health} details.
 */
public interface HealthService {
    /**
     * A sanity checker that tells us if the {@link org.glassfish.grizzly.http.server.HttpServer} has been started.
     * @return
     */
    boolean running();

    /**
     * A method that will start the {@link org.glassfish.grizzly.http.server.HttpServer} instance.
     * @throws IOException If there was an exception starting the server.
     */
    void start() throws IOException;

    /**
     * A method that will stop the {@link org.glassfish.grizzly.http.server.HttpServer} instance.
     */
    void stop();

    /**
     * Provides the {@link org.glassfish.jersey.server.Uri} the {@link org.glassfish.grizzly.http.server.HttpServer} is
     * configured to run at.
     */
    URI getUri();
}
