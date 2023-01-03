package io.pravega.controller.server.rest.generated.api;

import io.pravega.controller.server.rest.generated.model.*;
import io.pravega.controller.server.rest.generated.api.ScopesApiService;
import io.pravega.controller.server.rest.generated.api.factories.ScopesApiServiceFactory;

import io.swagger.annotations.ApiParam;
import io.swagger.jaxrs.*;

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

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.servlet.ServletConfig;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.*;
import javax.validation.constraints.*;

@Path("/scopes")


@io.swagger.annotations.Api(description = "the scopes API")

public class ScopesApi  {
   private final ScopesApiService delegate;

   public ScopesApi(@Context ServletConfig servletContext) {
      ScopesApiService delegate = null;

      if (servletContext != null) {
         String implClass = servletContext.getInitParameter("ScopesApi.implementation");
         if (implClass != null && !"".equals(implClass.trim())) {
            try {
               delegate = (ScopesApiService) Class.forName(implClass).getConstructor().newInstance();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         } 
      }

      if (delegate == null) {
         delegate = ScopesApiServiceFactory.getScopesApi();
      }

      this.delegate = delegate;
   }

    @POST
    
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new scope", response = ScopeProperty.class, tags={ "Scopes", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully created the scope", response = ScopeProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Scope with the given name already exists", response = ScopeProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a scope", response = ScopeProperty.class) })
    public Response createScope(@ApiParam(value = "The scope configuration" ,required=true) CreateScopeRequest createScopeRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.createScope(createScopeRequest,securityContext);
    }
    @POST
    @Path("/{scopeName}/streams")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Create a new stream", response = StreamProperty.class, tags={ "Streams", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "Successfully created the stream with the given configuration", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "Stream with given name already exists", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while creating a stream", response = StreamProperty.class) })
    public Response createStream(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "The stream configuration" ,required=true) CreateStreamRequest createStreamRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.createStream(scopeName,createStreamRequest,securityContext);
    }
    @DELETE
    @Path("/{scopeName}/readergroups/{readerGroupName}")
    
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a reader group", response = void.class, tags={ "ReaderGroups", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the reader group", response = void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or reader group with given name not found", response = void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting a reader", response = void.class) })
    public Response deleteReaderGroup(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Reader group name",required=true) @PathParam("readerGroupName") String readerGroupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteReaderGroup(scopeName,readerGroupName,securityContext);
    }
    @DELETE
    @Path("/{scopeName}")
    
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a scope", response = void.class, tags={ "Scopes", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the scope", response = void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = void.class),
        
        @io.swagger.annotations.ApiResponse(code = 412, message = "Cannot delete scope since it has non-empty list of streams", response = void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting a scope", response = void.class) })
    public Response deleteScope(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteScope(scopeName,securityContext);
    }
    @DELETE
    @Path("/{scopeName}/streams/{streamName}")
    
    
    @io.swagger.annotations.ApiOperation(value = "", notes = "Delete a stream", response = void.class, tags={ "Streams", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 204, message = "Successfully deleted the stream", response = void.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Stream not found", response = void.class),
        
        @io.swagger.annotations.ApiResponse(code = 412, message = "Cannot delete stream since it is not sealed", response = void.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while deleting the stream", response = void.class) })
    public Response deleteStream(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Stream name",required=true) @PathParam("streamName") String streamName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.deleteStream(scopeName,streamName,securityContext);
    }
    @GET
    @Path("/{scopeName}/readergroups/{readerGroupName}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing reader group", response = ReaderGroupProperty.class, tags={ "ReaderGroups", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found reader group properties", response = ReaderGroupProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or reader group with given name not found", response = ReaderGroupProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching reader group details", response = ReaderGroupProperty.class) })
    public Response getReaderGroup(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Reader group name",required=true) @PathParam("readerGroupName") String readerGroupName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getReaderGroup(scopeName,readerGroupName,securityContext);
    }
    @GET
    @Path("/{scopeName}/streams/{streamName}/scaling-events")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Get scaling events for a given datetime period.", response = ScalingEventList.class, tags={ "Streams", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Successfully fetched list of scaling events.", response = ScalingEventList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope/Stream not found.", response = ScalingEventList.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal Server error while fetching scaling events.", response = ScalingEventList.class) })
    public Response getScalingEvents(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Stream name",required=true) @PathParam("streamName") String streamName
,@ApiParam(value = "Parameter to display scaling events from that particular datetime. Input should be milliseconds from Jan 1 1970.",required=true) @QueryParam("from") Long from
,@ApiParam(value = "Parameter to display scaling events to that particular datetime. Input should be milliseconds from Jan 1 1970.",required=true) @QueryParam("to") Long to
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getScalingEvents(scopeName,streamName,from,to,securityContext);
    }
    @GET
    @Path("/{scopeName}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Retrieve details of an existing scope", response = ScopeProperty.class, tags={ "Scopes", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Successfully retrieved the scope details", response = ScopeProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope with the given name not found", response = ScopeProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching scope details", response = ScopeProperty.class) })
    public Response getScope(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getScope(scopeName,securityContext);
    }
    @GET
    @Path("/{scopeName}/streams/{streamName}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the properties of an existing stream", response = StreamProperty.class, tags={ "Streams", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Found stream properties", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or stream with given name not found", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching stream details", response = StreamProperty.class) })
    public Response getStream(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Stream name",required=true) @PathParam("streamName") String streamName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getStream(scopeName,streamName,securityContext);
    }
    @GET
    @Path("/{scopeName}/readergroups")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "List reader groups within the given scope", response = ReaderGroupsList.class, tags={ "ReaderGroups", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of all reader groups configured for the given scope", response = ReaderGroupsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = ReaderGroupsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of reader groups for the given scope", response = ReaderGroupsList.class) })
    public Response listReaderGroups(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.listReaderGroups(scopeName,securityContext);
    }
    @GET
    
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "List all available scopes in pravega", response = ScopesList.class, tags={ "Scopes", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of currently available scopes", response = ScopesList.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching list of scopes", response = ScopesList.class) })
    public Response listScopes(@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.listScopes(securityContext);
    }
    @GET
    @Path("/{scopeName}/streams")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "List streams within the given scope", response = StreamsList.class, tags={ "Streams", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "List of all streams configured for the given scope", response = StreamsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope not found", response = StreamsList.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the list of streams for the given scope", response = StreamsList.class) })
    public Response listStreams(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Filter options", allowableValues="showInternalStreams, tag") @QueryParam("filter_type") String filterType
,@ApiParam(value = "value to be passed. must match the type passed with it.") @QueryParam("filter_value") String filterValue
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.listStreams(scopeName,filterType,filterValue,securityContext);
    }
    @PUT
    @Path("/{scopeName}/streams/{streamName}")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Update configuration of an existing stream", response = StreamProperty.class, tags={ "Streams", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Successfully updated the stream configuration", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or stream with given name not found", response = StreamProperty.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while updating the stream", response = StreamProperty.class) })
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
    @io.swagger.annotations.ApiOperation(value = "", notes = "Updates the current state of the stream", response = StreamState.class, tags={ "Streams", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "Successfully updated the stream state", response = StreamState.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "Scope or stream with given name not found", response = StreamState.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while updating the stream state", response = StreamState.class) })
    public Response updateStreamState(@ApiParam(value = "Scope name",required=true) @PathParam("scopeName") String scopeName
,@ApiParam(value = "Stream name",required=true) @PathParam("streamName") String streamName
,@ApiParam(value = "The state info to be updated" ,required=true) StreamState updateStreamStateRequest
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.updateStreamState(scopeName,streamName,updateStreamStateRequest,securityContext);
    }
}
