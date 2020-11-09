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

public interface HealthServer {
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
