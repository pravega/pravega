package com.emc.pravega.controller.server.rest.generated.api;

import com.emc.pravega.controller.server.rest.generated.api.*;
import com.emc.pravega.controller.server.rest.generated.model.*;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import com.emc.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import com.emc.pravega.controller.server.rest.generated.model.InlineResponse201;
import com.emc.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.generated.model.StreamProperty;
import com.emc.pravega.controller.server.rest.generated.model.ScopesList;
import com.emc.pravega.controller.server.rest.generated.model.StreamsList;
import com.emc.pravega.controller.server.rest.generated.model.UpdateStreamRequest;

import java.util.List;
import com.emc.pravega.controller.server.rest.generated.api.NotFoundException;

import java.io.InputStream;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;


public abstract class ScopesApiService {
    public abstract Response createScope(CreateScopeRequest createScopeRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response createStream(String scopeName,CreateStreamRequest createStreamRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteScope(String scopeName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteStream(String scopeName,String streamName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getStream(String scopeName,String streamName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response listScopes(SecurityContext securityContext) throws NotFoundException;
    public abstract Response listStreams(String scopeName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response updateStream(String scopeName,String streamName,UpdateStreamRequest updateStreamRequest,SecurityContext securityContext) throws NotFoundException;
}
