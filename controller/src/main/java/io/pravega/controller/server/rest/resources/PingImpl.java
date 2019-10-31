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
import lombok.extern.slf4j.Slf4j;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/*
Implementation of Ping, which serves as a check for working REST server.
 */
@Slf4j
public class PingImpl implements ApiV1.Ping {

    /**
     * Implementation of Ping API.
     *
     * @return Response 200 OK
     */
    @Override
    public Response ping() {
        return Response.status(Status.OK).build();
    }
}
