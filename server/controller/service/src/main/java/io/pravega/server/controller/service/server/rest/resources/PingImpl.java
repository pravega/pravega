/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.server.rest.resources;

import io.pravega.server.controller.service.server.rest.v1.ApiV1;
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
