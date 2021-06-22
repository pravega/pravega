package io.pravega.controller.server.rest.generated.api;

import io.pravega.controller.server.rest.generated.api.*;
import io.pravega.controller.server.rest.generated.model.*;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import io.pravega.controller.server.rest.generated.model.HealthDetails;
import io.pravega.controller.server.rest.generated.model.HealthResult;
import io.pravega.controller.server.rest.generated.model.HealthStatus;

import java.util.List;
import io.pravega.controller.server.rest.generated.api.NotFoundException;

import java.io.InputStream;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;

public abstract class HealthApiService {
    public abstract Response getContributorDetails(String id,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getContributorHealth(String id,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getContributorLiveness(String id,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getContributorReadiness(String id,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getContributorStatus(String id,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getDetails(SecurityContext securityContext) throws NotFoundException;
    public abstract Response getHealth(SecurityContext securityContext) throws NotFoundException;
    public abstract Response getLiveness(SecurityContext securityContext) throws NotFoundException;
    public abstract Response getReadiness(SecurityContext securityContext) throws NotFoundException;
    public abstract Response getStatus(SecurityContext securityContext) throws NotFoundException;
}
