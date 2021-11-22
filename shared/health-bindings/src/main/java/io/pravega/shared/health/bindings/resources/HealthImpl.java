/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared.health.bindings.resources;

import io.pravega.auth.AuthException;
import io.pravega.shared.health.bindings.generated.api.NotFoundException;
import io.pravega.shared.health.bindings.generated.model.HealthDetails;
import io.pravega.shared.health.bindings.generated.model.HealthResult;
import io.pravega.shared.health.bindings.generated.model.HealthStatus;
import io.pravega.shared.health.bindings.v1.ApiV1;
import io.pravega.shared.health.ContributorNotFoundException;
import io.pravega.shared.rest.security.RESTAuthHelper;
import io.pravega.shared.rest.security.AuthHandlerManager;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthEndpoint;
import io.pravega.shared.health.Status;
import io.pravega.shared.security.auth.AuthorizationResource;
import io.pravega.shared.security.auth.AuthorizationResourceImpl;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import io.pravega.common.LoggerHelpers;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.pravega.auth.AuthHandler.Permissions.READ_UPDATE;

@Slf4j
public class HealthImpl implements ApiV1.HealthApi {

    @Context
    HttpHeaders headers;

    private final HealthEndpoint endpoint;

    private final RESTAuthHelper restAuthHelper;
    private final AuthorizationResource authorizationResource = new AuthorizationResourceImpl();

    public HealthImpl(AuthHandlerManager pravegaAuthManager, HealthEndpoint endpoint) {
        this.endpoint = endpoint;
        this.restAuthHelper = new RESTAuthHelper(pravegaAuthManager);
    }

    @Override
    public void getContributorHealth(String id, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getHealth(id, securityContext, asyncResponse, "getContributorHealth");
    }

    @Override
    public void getHealth(SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getHealth(null, securityContext, asyncResponse, "getHealth");
    }

    private void getHealth(String id, SecurityContext securityContext, AsyncResponse asyncResponse, String method) {
        long traceId = LoggerHelpers.traceEnter(log, method);
        processRequest(() -> {
            Health health = endpoint.getHealth(id);
            Response response = Response.status(Response.Status.OK)
                    .entity(adapter(health))
                    .build();
            asyncResponse.resume(response);
        }, asyncResponse, method, traceId, id);
    }

    @Override
    public void getContributorLiveness(String id, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getLiveness(id, securityContext, asyncResponse, "getContributorLiveness");
    }

    @Override
    public void getLiveness(SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getLiveness(null, securityContext, asyncResponse, "getLiveness");
    }

    private void getLiveness(String id, SecurityContext securityContext, AsyncResponse asyncResponse, String method) {
        long traceId = LoggerHelpers.traceEnter(log, method);
        processRequest(() -> {
            boolean alive = endpoint.isAlive(id);
            asyncResponse.resume(Response.status(Response.Status.OK)
                    .entity(alive)
                    .build());
        }, asyncResponse, method, traceId, id);
    }

    @Override
    public void getContributorDetails(String id, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getDetails(id, securityContext, asyncResponse, "getDetails");
    }

    @Override
    public void getDetails(SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getDetails(null, securityContext, asyncResponse, "getDetails");
    }

    private void getDetails(String id, SecurityContext securityContext, AsyncResponse asyncResponse, String method) {
        long traceId = LoggerHelpers.traceEnter(log, method);
        processRequest(() -> {
            restAuthHelper.authenticateAuthorize(getAuthorizationHeader(), authorizationResource.ofScopes(), READ_UPDATE);
            Map<String, Object> details = endpoint.getDetails(id);
            asyncResponse.resume(Response.status(Response.Status.OK)
                    .entity(adapter(details))
                    .build());
        }, asyncResponse, method, traceId, id);
    }

    @Override
    public void getContributorReadiness(String id, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getReadiness(id, securityContext, asyncResponse, "getContributorReadiness");
    }

    @Override
    public void getReadiness(SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getReadiness(null, securityContext, asyncResponse, "getReadiness");
    }

    private void getReadiness(String id, SecurityContext securityContext, AsyncResponse asyncResponse, String method) {
        long traceId = LoggerHelpers.traceEnter(log, method);
        processRequest(() -> {
            boolean ready = endpoint.isReady(id);
            asyncResponse.resume(Response.status(Response.Status.OK)
                    .entity(ready)
                    .build());
        }, asyncResponse, method, traceId, id);
    }

    @Override
    public void getContributorStatus(String id, SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getStatus(id, securityContext, asyncResponse, "getContributorStatus");
    }

    @Override
    public void getStatus(SecurityContext securityContext, AsyncResponse asyncResponse) throws NotFoundException {
        getStatus(null, securityContext, asyncResponse, "getStatus");
    }

    private void getStatus(String id, SecurityContext securityContext, AsyncResponse asyncResponse, String method) {
        long traceId = LoggerHelpers.traceEnter(log, method);
        processRequest(() -> {
            Status status = endpoint.getStatus(id);
            asyncResponse.resume(Response.status(Response.Status.OK)
                    .entity(adapter(status))
                    .build());
        }, asyncResponse, method, traceId, id);
    }

    /**
     * This is a shortcut for {@code headers.getRequestHeader().get(HttpHeaders.AUTHORIZATION)}.
     *
     * @return a list of read-only values of the HTTP Authorization header
     * @throws IllegalStateException if called outside the scope of the HTTP request
     */
    private List<String> getAuthorizationHeader() {
        return headers.getRequestHeader(HttpHeaders.AUTHORIZATION);
    }

    private void processRequest(Runnable request, AsyncResponse response, String method, long traceId, String id) {
        try {
            request.run();
        } catch (AuthException e) {
            log.warn("Failed to complete Health request for '{}' due to authentication failure.", id);
            response.resume(Response.status(Response.Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, method, traceId);
        } catch (ContributorNotFoundException e) {
            log.warn("No HealthContributor found with name '{}'.", id);
            response.resume(Response.status(Response.Status.fromStatusCode(e.getResponseCode())).build());
            LoggerHelpers.traceLeave(log, method, traceId);
        } finally {
            LoggerHelpers.traceLeave(log, method, traceId);
        }
    }

    // The following methods provide a means to cast the HealthService framework models, to the generated models.
    private static HealthResult adapter(Health health) {
        return new HealthResult()
                .name(health.getName())
                .status(adapter(health.getStatus()))
                .liveness(health.isAlive())
                .readiness(health.isReady())
                .details(adapter(health.getDetails()))
                .children(health.getChildren()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> adapter(entry.getValue()))));
    }

    private static HealthDetails adapter(Map<String, Object> details) {
        HealthDetails result = new HealthDetails();
        details.forEach((key, val) -> result.put(key, val.toString()));
        return result;
    }

    private static HealthStatus adapter(Status status) {
        return HealthStatus.fromValue(status.name());
    }

}