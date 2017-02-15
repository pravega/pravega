package com.emc.pravega.controller.server.rest.generated.api;

import com.emc.pravega.controller.server.rest.generated.model.*;
import com.emc.pravega.controller.server.rest.generated.api.ScopesApiService;
import com.emc.pravega.controller.server.rest.generated.api.factories.ScopesApiServiceFactory;

import io.swagger.annotations.ApiParam;
import io.swagger.jaxrs.*;

import com.emc.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.generated.model.StreamProperty;
import com.emc.pravega.controller.server.rest.generated.model.UpdateStreamRequest;

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
    @Path("/{scopeName}/streams")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Creates a new stream", response = StreamProperty.class, tags={  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successful created the stream", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Stream already exists", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = StreamProperty.class) })
    public Response createStream(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "The stream configuration" ,required=true) CreateStreamRequest createStreamRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.createStream(scopeName,createStreamRequest,securityContext);
    }
    @GET
    @Path("/{scopeName}/streams/{streamName}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the stream properties", response = StreamProperty.class, tags={  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found stream configuration", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Stream not found", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = StreamProperty.class) })
    public Response getStream(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Stream name",required=true) @PathParam("streamName") String streamName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getStream(scopeName,streamName,securityContext);
    }
    @PUT
    @Path("/{scopeName}/streams/{streamName}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "", response = StreamProperty.class, tags={  })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully updated the stream configuration", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Stream not found", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Server error", response = StreamProperty.class) })
    public Response updateStream(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Stream name",required=true) @PathParam("streamName") String streamName
,@ApiParam(value = "The new stream configuration" ,required=true) UpdateStreamRequest updateStreamRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.updateStream(scopeName,streamName,updateStreamRequest,securityContext);
    }
}
