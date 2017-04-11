/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega;

import com.emc.pravega.controller.server.rest.generated.api.JacksonJsonProvider;
import com.emc.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import com.emc.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import com.emc.pravega.controller.server.rest.generated.model.RetentionConfig;
import com.emc.pravega.controller.server.rest.generated.model.ScalingConfig;
import com.emc.pravega.controller.server.rest.generated.model.StreamProperty;
import com.emc.pravega.controller.server.rest.generated.model.ScopeProperty;
import com.emc.pravega.controller.server.rest.generated.model.StreamState;
import com.emc.pravega.controller.server.rest.generated.model.StreamsList;
import com.emc.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
import com.emc.pravega.framework.services.BookkeeperService;
import com.emc.pravega.framework.services.PravegaControllerService;
import com.emc.pravega.framework.services.PravegaSegmentStoreService;
import com.emc.pravega.framework.services.Service;
import com.emc.pravega.framework.services.ZookeeperService;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.utils.MarathonException;
import org.apache.commons.lang.RandomStringUtils;
import org.glassfish.jersey.client.ClientConfig;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static com.emc.pravega.controller.server.rest.generated.model.ScalingConfig.TypeEnum.FIXED_NUM_SEGMENTS;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.OK;
import static org.junit.Assert.assertEquals;

@Slf4j
public class ControllerRestApiTest {

    private final Client client;
    private WebTarget webTarget;
    private String restServerURI;
    private String resourceURl;

    public ControllerRestApiTest() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        client = ClientBuilder.newClient(clientConfig);
    }

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws InterruptedException If interrupted
     * @throws MarathonException    when error in setup
     * @throws URISyntaxException   If URI is invalid
     */
    public static void setup() throws InterruptedException, MarathonException, URISyntaxException {

        //1. check if zk is running, if not start it
        Service zkService = new ZookeeperService("zookeeper");
        if (!zkService.isRunning()) {
            zkService.start(true);
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to bk, host, controller
        URI zkUri = zkUris.get(0);
        //2, check if bk is running, otherwise start, get the zk ip
        Service bkService = new BookkeeperService("bookkeeper", zkUri);
        if (!bkService.isRunning()) {
            bkService.start(true);
        }

        List<URI> bkUris = bkService.getServiceDetails();
        log.debug("bookkeeper service details: {}", bkUris);

        //3. start controller
        Service conService = new PravegaControllerService("controller", zkUri);
        if (!conService.isRunning()) {
            conService.start(true);
        }

        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega Controller service details: {}", conUris);

        //4.start host
        Service segService = new PravegaSegmentStoreService("segmentstore", zkUri, conUris.get(0));
        if (!segService.isRunning()) {
            segService.start(true);
        }

        List<URI> segUris = segService.getServiceDetails();
        log.debug("pravega host service details: {}", segUris);
    }

    public void restApiTests() {

        Service conService = new PravegaControllerService("controller", null, 0, 0.0, 0.0);
        List<URI> ctlURIs = conService.getServiceDetails();
        URI controllerRESTUri = ctlURIs.get(1);
        Invocation.Builder builder;
        Response response;

        restServerURI = "http://" + controllerRESTUri.getHost() + ":" + controllerRESTUri.getPort();
        log.info("REST Server URI: {}", restServerURI);

        // TEST REST server status, ping test
        resourceURl = new StringBuilder(restServerURI).append("/ping").toString();
        webTarget = client.target(resourceURl);
        builder = webTarget.request();
        response = builder.get();
        assertEquals("Ping test", OK.getStatusCode(), response.getStatus());
        log.info("REST Server is running. Ping successful.");

        final String scope1 = RandomStringUtils.randomAlphanumeric(10);
        final String stream1 = RandomStringUtils.randomAlphanumeric(10);

        // TEST CreateScope POST http://controllerURI:Port/v1/scopes
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes").toString();
        webTarget = client.target(resourceURl);
        final CreateScopeRequest createScopeRequest = new CreateScopeRequest();
        createScopeRequest.setScopeName(scope1);
        builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        response = builder.post(Entity.json(createScopeRequest));

        assertEquals("Create scope status", CREATED.getStatusCode(), response.getStatus());
        assertEquals("Create scope response", scope1, response.readEntity(ScopeProperty.class).getScopeName());
        log.info("Create scope: {} successful ", scope1);

        // TEST CreateStream POST  http://controllerURI:Port/v1/scopes/{scopeName}/streams
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scope1 + "/streams").toString();
        webTarget = client.target(resourceURl);

        CreateStreamRequest createStreamRequest = new CreateStreamRequest();
        ScalingConfig scalingConfig = new ScalingConfig();
        scalingConfig.setType(FIXED_NUM_SEGMENTS);
        scalingConfig.setTargetRate(2);
        scalingConfig.scaleFactor(2);
        scalingConfig.minSegments(2);

        RetentionConfig retentionConfig = new RetentionConfig();
        retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_DAYS);
        retentionConfig.setValue(123L);

        createStreamRequest.setStreamName(stream1);
        createStreamRequest.setScalingPolicy(scalingConfig);
        createStreamRequest.setRetentionPolicy(retentionConfig);

        builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        response = builder.post(Entity.json(createStreamRequest));

        assertEquals("Create stream status", CREATED.getStatusCode(), response.getStatus());
        final StreamProperty streamPropertyResponse = response.readEntity(StreamProperty.class);
        assertEquals("Scope name in response", scope1, streamPropertyResponse.getScopeName());
        assertEquals("Stream name in response", stream1, streamPropertyResponse.getStreamName());
        log.info("Create stream: {} successful", stream1);

        // Test listScopes  GET http://controllerURI:Port/v1/scopes/{scopeName}/streams
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes").toString();
        webTarget = client.target(resourceURl);
        builder = webTarget.request();
        response = builder.get();
        assertEquals("List scopes", OK.getStatusCode(), response.getStatus());
        log.info("List scopes successful");

        // Test listStream GET /v1/scopes/scope1/streams
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scope1 + "/streams").toString();
        webTarget = client.target(resourceURl);
        builder = webTarget.request();
        response = builder.get();
        assertEquals("List streams", OK.getStatusCode(), response.getStatus());
        assertEquals("List streams size", 1, response.readEntity(StreamsList.class).getStreams().size());
        log.info("List streams successful");

        // Test getScope
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/"+scope1).toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get scope status", OK.getStatusCode(), response.getStatus());
        assertEquals("Get scope scope1 response", scope1, response.readEntity(ScopeProperty.class).getScopeName());
        log.info("Get scope successful");

        // Test updateStream
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/"+ scope1 + "/streams/"+stream1).toString();

        UpdateStreamRequest updateStreamRequest = new UpdateStreamRequest();
        ScalingConfig scalingConfig1 = new ScalingConfig();
        scalingConfig1.setType(FIXED_NUM_SEGMENTS);
        scalingConfig1.setTargetRate(2);
        scalingConfig1.scaleFactor(3); // update existing scaleFactor from 2 to 3
        scalingConfig1.minSegments(4); // update existing minSegments from 2 to 4
        updateStreamRequest.setScalingPolicy(scalingConfig1);
        updateStreamRequest.setRetentionPolicy(retentionConfig);

        response = client.target(resourceURl).request(MediaType.APPLICATION_JSON_TYPE)
                .put(Entity.json(updateStreamRequest));
        assertEquals("Update stream status", OK.getStatusCode(), response.getStatus());
        assertEquals("Verify updated property", 4, response.readEntity(StreamProperty.class)
                .getScalingPolicy().getMinSegments().intValue());
        log.info("Update stream successful");

        // Test getStream
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/"+ scope1 + "/streams/"+stream1).toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get stream status", OK.getStatusCode(), response.getStatus());
        assertEquals("Get stream stream1 response", stream1, response.readEntity(StreamProperty.class).getStreamName());
        log.info("Get stream successful");

        // Test updateStreamState
        resourceURl = new StringBuilder(restServerURI)
                .append("/v1/scopes/"+ scope1 + "/streams/"+stream1+"/state").toString();
        StreamState streamState = new StreamState();
        streamState.setStreamState(StreamState.StreamStateEnum.SEALED);
        response = client.target(resourceURl).request(MediaType.APPLICATION_JSON_TYPE)
                .put(Entity.json(streamState));
        assertEquals("UpdateStreamState status", OK.getStatusCode(), response.getStatus());
        assertEquals("UpdateStreamState status in response", streamState.getStreamState(),
                response.readEntity(StreamState.class).getStreamState());
        log.info("Update stream state successful");

        // Test deleteStream
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/"+ scope1 + "/streams/"+stream1).toString();
        response = client.target(resourceURl).request().delete();
        assertEquals("DeleteStream status", NO_CONTENT.getStatusCode(), response.getStatus());
        log.info("Delete stream successful");

        // Test deleteScope
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/"+scope1).toString();
        response = client.target(resourceURl).request().delete();
        assertEquals("Get scope status", NO_CONTENT.getStatusCode(), response.getStatus());
        log.info("Delete Scope successful");

        log.info("Test restApiTests passed successfully!");
    }
}
