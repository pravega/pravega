package io.pravega.segmentstore.server.host.rest.generated.api;

import io.pravega.segmentstore.server.host.rest.generated.api.factories.StreamsApiServiceFactory;
import io.pravega.segmentstore.server.host.rest.generated.api.StreamsApiService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.jaxrs.*;

import javax.servlet.ServletConfig;
import javax.validation.constraints.*;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

@Path("/streams")


@io.swagger.annotations.Api(description = "the streams API")

public class StreamsApi  {
   private final StreamsApiService delegate;

   public StreamsApi(@Context ServletConfig servletContext) {
      StreamsApiService delegate = null;

      if (servletContext != null) {
         String implClass = servletContext.getInitParameter("StreamsApi.implementation");
         if (implClass != null && !"".equals(implClass.trim())) {
            try {
               delegate = (StreamsApiService) Class.forName(implClass).getConstructor().newInstance();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         } 
      }

      if (delegate == null) {
         delegate = StreamsApiServiceFactory.getStreamsApi();
      }

      this.delegate = delegate;
   }
}
