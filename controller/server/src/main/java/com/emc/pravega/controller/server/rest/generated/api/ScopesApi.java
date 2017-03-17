package com.emc.pravega.controller.server.rest.generated.api;

import com.emc.pravega.controller.server.rest.generated.model.*;
import com.emc.pravega.controller.server.rest.generated.api.ScopesApiService;
import com.emc.pravega.controller.server.rest.generated.api.factories.ScopesApiServiceFactory;

import io.swagger.annotations.ApiParam;
import io.swagger.jaxrs.*;

import com.emc.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import com.emc.pravega.controller.server.rest.generated.model.ScopeProperty;
import com.emc.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.generated.model.StreamProperty;
import com.emc.pravega.controller.server.rest.generated.model.ScopesList;
import com.emc.pravega.controller.server.rest.generated.model.StreamsList;
import com.emc.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import com.emc.pravega.controller.server.rest.generated.model.StreamState;

import java.util.List;
import com.emc.pravega.controller.server.rest.generated.api.NotFoundException;

import java.io.InputStream;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;

@Path("/scopes")


@io.swagger.annotations.Api(description = "the scopes API")

public class ScopesApi  {
   private final ScopesApiService delegate = ScopesApiServiceFactory.getScopesApi();

    @POST
    
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Creates a new scope", response = ScopeProperty.class, tags={  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully created the scope", response = ScopeProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Scope already exists", response = ScopeProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = ScopeProperty.class) })
    public Response createScope(@ApiParam(value = "The scope configuration" ,required=true) CreateScopeRequest createScopeRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.createScope(createScopeRequest,securityContext);
    }
    @POST
    @Path("/{scopeName}/streams")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Creates a new stream", response = StreamProperty.class, tags={  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successful created the stream", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Stream already exists", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = StreamProperty.class) })
    public Response createStream(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "The stream configuration" ,required=true) CreateStreamRequest createStreamRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.createStream(scopeName,createStreamRequest,securityContext);
    }
    @DELETE
    @Path("/{scopeName}")
    
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a scope", response = void.class, tags={  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the scope", response = void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = void.class),
        
        @io.swagger.annotations.ApiResponse(code = 412, message = "Cannot delete scope which has non-empty list of streams", response = void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = void.class) })
    public Response deleteScope(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteScope(scopeName,securityContext);
    }
    @DELETE
    @Path("/{scopeName}/streams/{streamName}")
    
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a stream", response = void.class, tags={  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the stream", response = void.class),

        @io.swagger.annotations.ApiResponse(code = 404, message = "Stream not found", response = void.class),

        @io.swagger.annotations.ApiResponse(code = 412, message = "Cannot delete stream since it is not sealed", response = void.class),

        @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = void.class) })
    public Response deleteStream(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Stream name",required=true) @PathParam("streamName") String streamName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteStream(scopeName,streamName,securityContext);
    }
    @GET
    @Path("/{scopeName}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Retrieve scope", response = ScopeProperty.class, tags={  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Successfully retrieved the scope", response = ScopeProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = ScopeProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = ScopeProperty.class) })
    public Response getScope(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getScope(scopeName,securityContext);
    }
    @GET
    @Path("/{scopeName}/streams/{streamName}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the stream properties", response = StreamProperty.class, tags={  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found stream configuration", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or stream not found", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = StreamProperty.class) })
    public Response getStream(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Stream name",required=true) @PathParam("streamName") String streamName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getStream(scopeName,streamName,securityContext);
    }
    @GET
    
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "List all scopes in the system", response = ScopesList.class, tags={  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of scope objects", response = ScopesList.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = ScopesList.class) })
    public Response listScopes(@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.listScopes(securityContext);
    }
    @GET
    @Path("/{scopeName}/streams")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "List streams within the given scope", response = StreamsList.class, tags={  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of stream objects", response = StreamsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = StreamsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = StreamsList.class) })
    public Response listStreams(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.listStreams(scopeName,securityContext);
    }
    @PUT
    @Path("/{scopeName}/streams/{streamName}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "", response = StreamProperty.class, tags={  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Successfully updated the stream configuration", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or stream not found", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = StreamProperty.class) })
    public Response updateStream(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Stream name",required=true) @PathParam("streamName") String streamName
,@ApiParam(value = "The new stream configuration" ,required=true) UpdateStreamRequest updateStreamRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.updateStream(scopeName,streamName,updateStreamRequest,securityContext);
    }
    @PUT
    @Path("/{scopeName}/streams/{streamName}/state")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Updates the current state of the stream", response = StreamState.class, tags={  })
    @io.swagger.annotations.ApiResponses(value = {
        @io.swagger.annotations.ApiResponse(code = 200, message = "Successfully updated the stream state", response = StreamState.class),

        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or stream not found", response = StreamState.class),

        @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = StreamState.class) })
    public Response updateStreamState(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Stream name",required=true) @PathParam("streamName") String streamName
,@ApiParam(value = "The state info to be updated" ,required=true) StreamState updateStreamStateRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.updateStreamState(scopeName,streamName,updateStreamStateRequest,securityContext);
    }
}
