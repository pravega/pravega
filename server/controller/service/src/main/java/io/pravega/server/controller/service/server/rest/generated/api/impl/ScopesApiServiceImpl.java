package io.pravega.server.controller.service.server.rest.generated.api.impl;

import io.pravega.server.controller.service.server.rest.generated.api.ApiResponseMessage;
import io.pravega.server.controller.service.server.rest.generated.api.ScopesApiService;
import io.pravega.server.controller.service.server.rest.generated.model.CreateScopeRequest;
import io.pravega.server.controller.service.server.rest.generated.model.CreateStreamRequest;
import io.pravega.server.controller.service.server.rest.generated.model.UpdateStreamRequest;
import io.pravega.server.controller.service.server.rest.generated.model.StreamState;

import io.pravega.server.controller.service.server.rest.generated.api.NotFoundException;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;


public class ScopesApiServiceImpl extends ScopesApiService {
    @Override
    public Response createScope(CreateScopeRequest createScopeRequest, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response createStream(String scopeName, CreateStreamRequest createStreamRequest, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response deleteScope(String scopeName, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response deleteStream(String scopeName, String streamName, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response getScope(String scopeName, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response getStream(String scopeName, String streamName, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response listScopes(SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response listStreams(String scopeName, String showInternalStreams, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response updateStream(String scopeName, String streamName, UpdateStreamRequest updateStreamRequest, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response updateStreamState(String scopeName, String streamName, StreamState updateStreamStateRequest, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
}
