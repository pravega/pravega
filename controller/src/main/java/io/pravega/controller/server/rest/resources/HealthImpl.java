/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rest.resources;

import io.pravega.controller.server.rest.v1.ApiV1;
import io.pravega.shared.healthcheck.HealthInfo;
import io.pravega.shared.healthcheck.HealthRegistryImpl;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.Optional;

/**
 * Implementation of Health, which serves as a HealthCheck for Pravega Controller instance.
 */
@Slf4j
public class HealthImpl implements ApiV1.Health {

    /**
     * Implementation of Health PI.
     *
     * @return Response 200 OK
     */
    @Override
    public Response health() {

        Optional<HealthInfo> health = HealthRegistryImpl.getInstance().checkHealth();
        if (health.isPresent()) {
            Status status = health.get().getStatus() == HealthInfo.Status.HEALTH ? Status.OK : Status.INTERNAL_SERVER_ERROR;
            return Response.status(status).entity(health.get().getDetails()).build();
        } else {
            return Response.status(Status.OK).entity("No HealthInfo available").build();
        }
    }
}
