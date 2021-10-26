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
package io.pravega.test.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.controller.server.rest.generated.api.JacksonJsonProvider;
import io.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import io.pravega.controller.server.rest.generated.model.CreateStreamRequest;
import io.pravega.controller.server.rest.generated.model.ReaderGroupProperty;
import io.pravega.controller.server.rest.generated.model.ReaderGroupsList;
import io.pravega.controller.server.rest.generated.model.ReaderGroupsListReaderGroups;
import io.pravega.controller.server.rest.generated.model.RetentionConfig;
import io.pravega.controller.server.rest.generated.model.ScalingConfig;
import io.pravega.controller.server.rest.generated.model.ScopeProperty;
import io.pravega.controller.server.rest.generated.model.StreamProperty;
import io.pravega.controller.server.rest.generated.model.StreamState;
import io.pravega.controller.server.rest.generated.model.StreamsList;
import io.pravega.controller.server.rest.generated.model.TagsList;
import io.pravega.controller.server.rest.generated.model.UpdateStreamRequest;
import io.pravega.controller.store.stream.ScaleMetadata;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.integration.utils.SetupUtils;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ControllerRestApiTest {
    // Setup utility.
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    @Rule
    public final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    private final Client client;
    private WebTarget webTarget;
    private String restServerURI;
    private String resourceURl;

    public ControllerRestApiTest() {
        org.glassfish.jersey.client.ClientConfig clientConfig = new org.glassfish.jersey.client.ClientConfig();
        clientConfig.register(JacksonJsonProvider.class);
        clientConfig.property("sun.net.http.allowRestrictedHeaders", "true");
        client = ClientBuilder.newClient(clientConfig);
    }

    @BeforeClass
    public static void setupClass() throws Exception {
        SETUP_UTILS.startAllServices();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void restApiTests() {

        Invocation.Builder builder;
        Response response;

        restServerURI = SETUP_UTILS.getControllerRestUri().toString();
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
        Assert.assertEquals("Create scope response", scope1, response.readEntity(ScopeProperty.class).getScopeName());
        log.info("Create scope: {} successful ", scope1);

        // Create another scope for empty stream test later.
        final String scope2 = RandomStringUtils.randomAlphanumeric(10);
        final CreateScopeRequest createScopeRequest1 = new CreateScopeRequest();
        createScopeRequest1.setScopeName(scope2);
        builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        response = builder.post(Entity.json(createScopeRequest1));
        assertEquals("Create scope status", CREATED.getStatusCode(), response.getStatus());
        Assert.assertEquals("Create scope response", scope2, response.readEntity(ScopeProperty.class).getScopeName());

        // TEST CreateStream POST  http://controllerURI:Port/v1/scopes/{scopeName}/streams
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scope1 + "/streams").toString();
        webTarget = client.target(resourceURl);

        CreateStreamRequest createStreamRequest = new CreateStreamRequest();
        ScalingConfig scalingConfig = new ScalingConfig();
        scalingConfig.setType(ScalingConfig.TypeEnum.FIXED_NUM_SEGMENTS);
        scalingConfig.setTargetRate(2);
        scalingConfig.scaleFactor(2);
        scalingConfig.minSegments(2);

        RetentionConfig retentionConfig = new RetentionConfig();
        retentionConfig.setType(RetentionConfig.TypeEnum.LIMITED_DAYS);
        retentionConfig.setValue(123L);

        TagsList tagsList = new TagsList();
        tagsList.add("testTag");

        createStreamRequest.setStreamName(stream1);
        createStreamRequest.setScalingPolicy(scalingConfig);
        createStreamRequest.setRetentionPolicy(retentionConfig);
        createStreamRequest.setStreamTags(tagsList);
        createStreamRequest.setTimestampAggregationTimeout(1000L);
        createStreamRequest.setRolloverSizeBytes(1024L);

        builder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
        response = builder.post(Entity.json(createStreamRequest));

        assertEquals("Create stream status", CREATED.getStatusCode(), response.getStatus());
        final StreamProperty streamPropertyResponse = response.readEntity(StreamProperty.class);
        assertEquals("Scope name in response", scope1, streamPropertyResponse.getScopeName());
        assertEquals("Stream name in response", stream1, streamPropertyResponse.getStreamName());
        assertEquals("TimestampAggregationTimeout in response", 1000L, (long) streamPropertyResponse.getTimestampAggregationTimeout());
        assertEquals("RolloverSizeBytes in response", 1024L, (long) streamPropertyResponse.getRolloverSizeBytes());

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
        Assert.assertEquals("List streams size", 1, response.readEntity(StreamsList.class).getStreams().size());
        log.info("List streams successful");

        // Test listStream GET /v1/scopes/scope1/streams for tags
        response = client.target(resourceURl).queryParam("filter_type", "tag").
                queryParam("filter_value", "testTag").request().get();
        assertEquals("List streams", OK.getStatusCode(), response.getStatus());
        Assert.assertEquals("List streams size", 1, response.readEntity(StreamsList.class).getStreams().size());

        response = client.target(resourceURl).queryParam("filter_type", "tag").
                queryParam("filter_value", "randomTag").request().get();
        assertEquals("List streams", OK.getStatusCode(), response.getStatus());
        Assert.assertEquals("List streams size", 0, response.readEntity(StreamsList.class).getStreams().size());
        log.info("List streams with tag successful");

        response = client.target(resourceURl).queryParam("filter_type", "showInternalStreams").request().get();
        assertEquals("List streams", OK.getStatusCode(), response.getStatus());
        assertTrue(response.readEntity(StreamsList.class).getStreams().get(0).getStreamName().startsWith("_MARK"));
        log.info("List streams with showInternalStreams successful");

        // Test for the case when the scope is empty.
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scope2 + "/streams").toString();
        response = client.target(resourceURl).request().get();
        assertEquals("List streams", OK.getStatusCode(), response.getStatus());
        Assert.assertEquals("List streams size", 0, response.readEntity(StreamsList.class).getStreams().size());

        // Test getScope
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scope1).toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get scope status", OK.getStatusCode(), response.getStatus());
        assertEquals("Get scope scope1 response", scope1, response.readEntity(ScopeProperty.class).getScopeName());
        log.info("Get scope successful");

        // Test updateStream
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scope1 + "/streams/" + stream1)
                                                      .toString();

        UpdateStreamRequest updateStreamRequest = new UpdateStreamRequest();
        ScalingConfig scalingConfig1 = new ScalingConfig();
        scalingConfig1.setType(ScalingConfig.TypeEnum.FIXED_NUM_SEGMENTS);
        scalingConfig1.setTargetRate(2);
        scalingConfig1.scaleFactor(3); // update existing scaleFactor from 2 to 3
        scalingConfig1.minSegments(4); // update existing minSegments from 2 to 4
        updateStreamRequest.setScalingPolicy(scalingConfig1);
        updateStreamRequest.setRetentionPolicy(retentionConfig);
        updateStreamRequest.setTimestampAggregationTimeout(2000L);
        updateStreamRequest.setRolloverSizeBytes(2048L);

        response = client.target(resourceURl).request(MediaType.APPLICATION_JSON_TYPE)
                .put(Entity.json(updateStreamRequest));
        assertEquals("Update stream status", OK.getStatusCode(), response.getStatus());
        assertEquals("Verify updated property", 4, response.readEntity(StreamProperty.class)
                .getScalingPolicy().getMinSegments().intValue());
        log.info("Update stream successful");

        // Test scaling event list GET /v1/scopes/scope1/streams/stream1
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scope1 + "/streams/" + stream1
                + "/scaling-events").toString();
        response = client.target(resourceURl).queryParam("from", 0L).
                queryParam("to", System.currentTimeMillis()).request().get();
        List<ScaleMetadata> scaleMetadataListResponse = response.readEntity(
                new GenericType<List<ScaleMetadata>>() { });
        assertEquals(2, scaleMetadataListResponse.size());
        assertEquals(2, scaleMetadataListResponse.get(0).getSegments().size());
        assertEquals(4, scaleMetadataListResponse.get(1).getSegments().size());

        // Test getStream
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scope1 + "/streams/" + stream1)
                                                      .toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get stream status", OK.getStatusCode(), response.getStatus());
        StreamProperty responseProperty = response.readEntity(StreamProperty.class);
        assertEquals("Get stream stream1 response", stream1, responseProperty.getStreamName());
        assertEquals("Get stream stream1 response TimestampAggregationTimeout", (long) responseProperty.getTimestampAggregationTimeout(), 2000L);
        assertEquals("Get stream stream1 RolloverSizeBytes", (long) responseProperty.getRolloverSizeBytes(), 2048L);
        log.info("Get stream successful");

        // Test updateStreamState
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scope1 + "/streams/" + stream1 + "/state")
                                                      .toString();
        StreamState streamState = new StreamState();
        streamState.setStreamState(StreamState.StreamStateEnum.SEALED);
        response = client.target(resourceURl).request(MediaType.APPLICATION_JSON_TYPE)
                .put(Entity.json(streamState));
        assertEquals("UpdateStreamState status", OK.getStatusCode(), response.getStatus());
        assertEquals("UpdateStreamState status in response", streamState.getStreamState(),
                response.readEntity(StreamState.class).getStreamState());
        log.info("Update stream state successful");

        // Test deleteStream
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scope1 + "/streams/" + stream1)
                                                      .toString();
        response = client.target(resourceURl).request().delete();
        assertEquals("DeleteStream status", NO_CONTENT.getStatusCode(), response.getStatus());
        log.info("Delete stream successful");

        // Test deleteScope
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + scope1).toString();
        response = client.target(resourceURl).request().delete();
        assertEquals("Get scope status", NO_CONTENT.getStatusCode(), response.getStatus());
        log.info("Delete Scope successful");

        // Test reader groups APIs.
        // Prepare the streams and readers using the admin client.
        final String testScope = RandomStringUtils.randomAlphanumeric(10);
        final String testStream1 = RandomStringUtils.randomAlphanumeric(10);
        final String testStream2 = RandomStringUtils.randomAlphanumeric(10);
        URI controllerUri = SETUP_UTILS.getControllerUri();
        @Cleanup("shutdown")
        InlineExecutor inlineExecutor = new InlineExecutor();
        ClientConfig clientConfig = ClientConfig.builder().build();
        try (ConnectionPool cp = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
             StreamManager streamManager = new StreamManagerImpl(createController(controllerUri, inlineExecutor), cp)) {
            log.info("Creating scope: {}", testScope);
            streamManager.createScope(testScope);

            log.info("Creating stream: {}", testStream1);
            StreamConfiguration streamConf1 = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
            streamManager.createStream(testScope, testStream1, streamConf1);

            log.info("Creating stream: {}", testStream2);
            StreamConfiguration streamConf2 = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
            streamManager.createStream(testScope, testStream2, streamConf2);
        }

        final String readerGroupName1 = RandomStringUtils.randomAlphanumeric(10);
        final String readerGroupName2 = RandomStringUtils.randomAlphanumeric(10);
        final String reader1 = RandomStringUtils.randomAlphanumeric(10);
        final String reader2 = RandomStringUtils.randomAlphanumeric(10);
        try (ClientFactoryImpl clientFactory = new ClientFactoryImpl(testScope, createController(controllerUri, inlineExecutor), clientConfig);
             ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(testScope,
                     ClientConfig.builder().controllerURI(controllerUri).build())) {
            readerGroupManager.createReaderGroup(readerGroupName1, ReaderGroupConfig.builder()
                                                                                    .stream(Stream.of(testScope, testStream1))
                                                                                    .stream(Stream.of(testScope, testStream2))
                                                                                    .build());
            readerGroupManager.createReaderGroup(readerGroupName2, ReaderGroupConfig.builder()
                                                                                    .stream(Stream.of(testScope, testStream1))
                                                                                    .stream(Stream.of(testScope, testStream2))
                                                                                    .build());
            clientFactory.createReader(reader1, readerGroupName1, new JavaSerializer<Long>(),
                    ReaderConfig.builder().build());
            clientFactory.createReader(reader2, readerGroupName1, new JavaSerializer<Long>(),
                    ReaderConfig.builder().build());
        }

        // Test fetching readergroups.
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + testScope + "/readergroups").toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get readergroups status", OK.getStatusCode(), response.getStatus());
        ReaderGroupsList readerGroupsList = response.readEntity(ReaderGroupsList.class);
        assertEquals("Get readergroups size", 2, readerGroupsList.getReaderGroups().size());
        assertTrue(readerGroupsList.getReaderGroups().contains(
                new ReaderGroupsListReaderGroups().readerGroupName(readerGroupName1)));
        assertTrue(readerGroupsList.getReaderGroups().contains(
                new ReaderGroupsListReaderGroups().readerGroupName(readerGroupName2)));
        log.info("Get readergroups successful");

        // Test fetching readergroup info.
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + testScope + "/readergroups/"
                + readerGroupName1).toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get readergroup properties status", OK.getStatusCode(), response.getStatus());
        ReaderGroupProperty readerGroupProperty = response.readEntity(ReaderGroupProperty.class);
        assertEquals("Get readergroup name", readerGroupName1, readerGroupProperty.getReaderGroupName());
        assertEquals("Get readergroup scope name", testScope, readerGroupProperty.getScopeName());
        assertEquals("Get readergroup streams size", 2, readerGroupProperty.getStreamList().size());
        assertTrue(readerGroupProperty.getStreamList().contains(Stream.of(testScope, testStream1).getScopedName()));
        assertTrue(readerGroupProperty.getStreamList().contains(Stream.of(testScope, testStream2).getScopedName()));
        assertEquals("Get readergroup onlinereaders size", 2, readerGroupProperty.getOnlineReaderIds().size());
        assertTrue(readerGroupProperty.getOnlineReaderIds().contains(reader1));
        assertTrue(readerGroupProperty.getOnlineReaderIds().contains(reader2));

        // Test readergroup or scope not found.
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + testScope + "/readergroups/" +
                "unknownreadergroup").toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get readergroup properties status", NOT_FOUND.getStatusCode(), response.getStatus());
        resourceURl = new StringBuilder(restServerURI).append("/v1/scopes/" + "unknownscope" + "/readergroups/" +
                readerGroupName1).toString();
        response = client.target(resourceURl).request().get();
        assertEquals("Get readergroup properties status", NOT_FOUND.getStatusCode(), response.getStatus());
        log.info("Get readergroup properties successful");

        log.info("Test restApiTests passed successfully!");
    }

    private Controller createController(URI controllerUri, InlineExecutor executor) {
        return new ControllerImpl(ControllerImplConfig.builder()
                                                      .clientConfig(ClientConfig.builder().controllerURI(controllerUri).build())
                                                      .retryAttempts(1).build(), executor);
    }
}
