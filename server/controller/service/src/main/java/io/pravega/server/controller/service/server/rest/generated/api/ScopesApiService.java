package io.pravega.server.controller.service.server.rest.generated.api;

import io.pravega.server.controller.service.server.rest.generated.model.CreateScopeRequest;
import io.pravega.server.controller.service.server.rest.generated.model.CreateStreamRequest;

import io.pravega.server.controller.service.server.rest.generated.model.UpdateStreamRequest;
import io.pravega.server.controller.service.server.rest.generated.model.StreamState;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;


public abstract class ScopesApiService {
    public abstract Response createScope(CreateScopeRequest createScopeRequest, SecurityContext securityContext) throws NotFoundException;
    public abstract Response createStream(String scopeName, CreateStreamRequest createStreamRequest, SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteScope(String scopeName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteStream(String scopeName,String streamName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getScope(String scopeName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getStream(String scopeName,String streamName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response listScopes(SecurityContext securityContext) throws NotFoundException;
    public abstract Response listStreams(String scopeName,String showInternalStreams,SecurityContext securityContext) throws NotFoundException;
    public abstract Response updateStream(String scopeName, String streamName, UpdateStreamRequest updateStreamRequest, SecurityContext securityContext) throws NotFoundException;
    public abstract Response updateStreamState(String scopeName, String streamName, StreamState updateStreamStateRequest, SecurityContext securityContext) throws NotFoundException;
}
