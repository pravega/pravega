package com.emc.pravega.controller.server.rest.generated.api.impl;

import com.emc.pravega.controller.server.rest.generated.api.*;
import com.emc.pravega.controller.server.rest.generated.model.*;

import com.emc.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.generated.model.StreamProperty;
import com.emc.pravega.controller.server.rest.generated.model.UpdateStreamRequest;

import java.util.List;
import com.emc.pravega.controller.server.rest.generated.api.NotFoundException;

import java.io.InputStream;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;


public class ScopesApiServiceImpl extends ScopesApiService {
    @Override
    public Response createStream(String scopeName, CreateStreamRequest createStreamRequest, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response getStream(String scopeName, String streamName, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response updateStream(String scopeName, String streamName, UpdateStreamRequest updateStreamRequest, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
}
