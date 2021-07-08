package io.pravega.shared.health.bindings.generated.api;

import io.pravega.shared.health.bindings.generated.api.factories.HealthApiServiceFactory;
import io.pravega.shared.health.bindings.generated.model.HealthDetails;
import io.pravega.shared.health.bindings.generated.model.HealthResult;
import io.pravega.shared.health.bindings.generated.model.HealthStatus;
import io.swagger.annotations.ApiParam;

import javax.servlet.ServletConfig;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

@Path("/health")


@io.swagger.annotations.Api(description = "the health API")

public class HealthApi  {
   private final HealthApiService delegate;

   public HealthApi(@Context ServletConfig servletContext) {
      HealthApiService delegate = null;

      if (servletContext != null) {
         String implClass = servletContext.getInitParameter("HealthApi.implementation");
         if (implClass != null && !"".equals(implClass.trim())) {
            try {
               delegate = (HealthApiService) Class.forName(implClass).getConstructor().newInstance();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         } 
      }

      if (delegate == null) {
         delegate = HealthApiServiceFactory.getHealthApi();
      }

      this.delegate = delegate;
   }

    @GET
    @Path("/details/{id}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the details of a specific health contributor.", response = HealthDetails.class, tags={ "Health", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "The list of details for the health contributor with a given id.", response = HealthDetails.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "The health details for the contributor with given id was not found.", response = HealthDetails.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the health details for a given health contributor.", response = HealthDetails.class) })
    public Response getContributorDetails(@ApiParam(value = "The id of an existing health contributor.",required=true) @PathParam("id") String id
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getContributorDetails(id,securityContext);
    }
    @GET
    @Path("/{id}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Return the Health of a health contributor with a given id.", response = HealthResult.class, tags={ "Health", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "The Health result of the Controller.", response = HealthResult.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "A health provider for the given id could not be found.", response = HealthResult.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the health for a given contributor.", response = HealthResult.class) })
    public Response getContributorHealth(@ApiParam(value = "The id of an existing health contributor.",required=true) @PathParam("id") String id
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getContributorHealth(id,securityContext);
    }
    @GET
    @Path("/liveness/{id}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the liveness state of the specified health contributor.", response = Boolean.class, tags={ "Health", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "The alive status for the specified health contributor.", response = Boolean.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "The liveness status for the contributor with given id was not found.", response = Boolean.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the liveness state for a given health contributor.", response = Boolean.class) })
    public Response getContributorLiveness(@ApiParam(value = "The id of an existing health contributor.",required=true) @PathParam("id") String id
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getContributorLiveness(id,securityContext);
    }
    @GET
    @Path("/readiness/{id}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the ready state of the health contributor.", response = Boolean.class, tags={ "Health", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "The readiness status for the health contributor with given id.", response = Boolean.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "The readiness status for the contributor with given id was not found.", response = Boolean.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the ready state for a given health contributor.", response = Boolean.class) })
    public Response getContributorReadiness(@ApiParam(value = "The id of an existing health contributor.",required=true) @PathParam("id") String id
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getContributorReadiness(id,securityContext);
    }
    @GET
    @Path("/status/{id}")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the status of a specific health contributor.", response = HealthStatus.class, tags={ "Health", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "The health status of the Controller.", response = HealthStatus.class),
        
        @io.swagger.annotations.ApiResponse(code = 404, message = "The health status for the contributor with given id was not found.", response = HealthStatus.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the health status of a given health contributor.", response = HealthStatus.class) })
    public Response getContributorStatus(@ApiParam(value = "The id of an existing health contributor.",required=true) @PathParam("id") String id
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getContributorStatus(id,securityContext);
    }
    @GET
    @Path("/details")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the details of the Controller service.", response = HealthDetails.class, tags={ "Health", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "The list of details.", response = HealthDetails.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the health details of the Controller.", response = HealthDetails.class) })
    public Response getDetails(@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getDetails(securityContext);
    }
    @GET
    
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Return the Health of the Controller service.", response = HealthResult.class, tags={ "Health", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "The Health result of the Controller.", response = HealthResult.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the Health.", response = HealthResult.class) })
    public Response getHealth(@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getHealth(securityContext);
    }
    @GET
    @Path("/liveness")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the liveness state of the Controller service.", response = Boolean.class, tags={ "Health", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "The alive status.", response = Boolean.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the liveness state of the Controller.", response = Boolean.class) })
    public Response getLiveness(@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getLiveness(securityContext);
    }
    @GET
    @Path("/readiness")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the ready state of the Controller service.", response = Boolean.class, tags={ "Health", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "The ready status.", response = Boolean.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the ready state of the Controller.", response = Boolean.class) })
    public Response getReadiness(@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getReadiness(securityContext);
    }
    @GET
    @Path("/status")
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "", notes = "Fetch the status of the Controller service.", response = HealthStatus.class, tags={ "Health", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "The health status of the Controller.", response = HealthStatus.class),
        
        @io.swagger.annotations.ApiResponse(code = 500, message = "Internal server error while fetching the health status of the Controller.", response = HealthStatus.class) })
    public Response getStatus(@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getStatus(securityContext);
    }
}
