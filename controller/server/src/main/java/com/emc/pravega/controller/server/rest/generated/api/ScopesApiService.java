package com.emc.pravega.controller.server.rest.generated.api;

import com.emc.pravega.controller.server.rest.generated.api.*;
import com.emc.pravega.controller.server.rest.generated.model.*;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import com.emc.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.generated.model.StreamProperty;
import com.emc.pravega.controller.server.rest.generated.model.UpdateStreamRequest;

import java.util.List;
import com.emc.pravega.controller.server.rest.generated.api.NotFoundException;

import java.io.InputStream;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

@javax.annotation.Generated(value = "class io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2017-02-15T05:33:34.934-08:00")
public abstract class ScopesApiService {
    public abstract Response createStream(String scopeName,CreateStreamRequest createStreamRequest,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getStream(String scopeName,String streamName,SecurityContext securityContext) throws NotFoundException;
    public abstract Response updateStream(String scopeName,String streamName,UpdateStreamRequest updateStreamRequest,SecurityContext securityContext) throws NotFoundException;
}
