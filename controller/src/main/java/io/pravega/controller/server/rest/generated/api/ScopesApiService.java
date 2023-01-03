package io.pravega.controller.server.rest.generated.api;

import io.pravega.controller.server.rest.generated.api.*;
import io.pravega.controller.server.rest.generated.model.*;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import io.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import io.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import io.pravega.controller.server.rest.generated.model.ReaderGroupProperty;
import io.pravega.controller.server.rest.generated.model.ReaderGroupsList;
import io.pravega.controller.server.rest.generated.model.ScalingEventList;
import io.pravega.controller.server.rest.generated.model.ScopeProperty;
import io.pravega.controller.server.rest.generated.model.ScopesList;
import io.pravega.controller.server.rest.generated.model.StreamProperty;
import io.pravega.controller.server.rest.generated.model.StreamState;
import io.pravega.controller.server.rest.generated.model.StreamsList;
import io.pravega.controller.server.rest.generated.model.UpdateStreamRequest;

import java.util.List;
import io.pravega.controller.server.rest.generated.api.NotFoundException;

import java.io.InputStream;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;

public abstract class ScopesApiService {
    public abstract Response createScope(CreateScopeRequest createScopeRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response createStream(String scopeName,CreateStreamRequest createStreamRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteReaderGroup(String scopeName,String readerGroupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteScope(String scopeName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response deleteStream(String scopeName,String streamName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getReaderGroup(String scopeName,String readerGroupName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getScalingEvents(String scopeName,String streamName, @NotNull Long from, @NotNull Long to,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getScope(String scopeName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getStream(String scopeName,String streamName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response listReaderGroups(String scopeName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response listScopes(SecurityContext securityContext) throws NotFoundException;
    public abstract Response listStreams(String scopeName, String filterType, String filterValue,SecurityContext securityContext) throws NotFoundException;
    public abstract Response updateStream(String scopeName,String streamName,UpdateStreamRequest updateStreamRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response updateStreamState(String scopeName,String streamName,StreamState updateStreamStateRequest,SecurityContext securityContext) throws NotFoundException;
}
