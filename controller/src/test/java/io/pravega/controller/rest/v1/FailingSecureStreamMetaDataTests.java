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
package io.pravega.controller.rest.v1;

import io.grpc.ServerBuilder;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import io.pravega.controller.server.rest.generated.model.StreamState;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.shared.rest.security.AuthHandlerManager;
import io.pravega.test.common.TestUtils;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

public class FailingSecureStreamMetaDataTests extends StreamMetaDataTests {
    protected int expectedResult = 401;

    @Override
    @Before
    public void setup() throws Exception {
        this.authManager = new AuthHandlerManager(GRPCServerConfigImpl.builder()
                                                                      .authorizationEnabled(true)
                                                                      .tlsCertFile(SecurityConfigDefaults.TLS_SERVER_CERT_PATH)
                                                                      .tlsKeyFile(SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_PATH)
                                                                      .userPasswordFile(SecurityConfigDefaults.AUTH_HANDLER_INPUT_PATH)
                                                                      .port(1000)
                                                                      .build());
        ServerBuilder<?> server = ServerBuilder.forPort(TestUtils.getAvailableListenPort());
        GrpcAuthHelper.registerInterceptors(authManager.getHandlerMap(), server);
        super.setup();
    }

    @Override
    @Test
    public void testCreateStream() {
        String streamResourceURI = getURI() + "v1/scopes/" + scope1 + "/streams";
        Response response = addAuthHeaders(client.target(streamResourceURI).request()).buildPost(Entity.json(createStreamRequest)).invoke();
        assertEquals("Create Stream Status", expectedResult, response.getStatus());
        response.close();
    }

    @Override
    @Test
    public void testUpdateStreamState() throws Exception {
        final String resourceURI = getURI() + "v1/scopes/scope1/streams/stream1/state";
        StreamState streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(streamState)).invoke();
        assertEquals("Update Stream State response code", expectedResult, response.getStatus());
        response.close();
    }

    @Override
    @Test
    public void testDeleteScope() throws ExecutionException, InterruptedException {
        final String resourceURI = getURI() + "v1/scopes/scope1";

        // Test to delete a scope.
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Scope response code", expectedResult, response.getStatus());
        response.close();
    }

    @Override
    @Test
    public void testGetScope() throws ExecutionException, InterruptedException {
        final String resourceURI = getURI() + "v1/scopes/scope1";
        final String resourceURI2 = getURI() + "v1/scopes/scope2";

        // Test to get existent scope
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get existent scope", expectedResult, response.getStatus());
        response.close();
    }

    @Override
    @Test
    public void testCreateScope() throws ExecutionException, InterruptedException {
        final CreateScopeRequest createScopeRequest = new CreateScopeRequest().scopeName(scope1);
        final String resourceURI = getURI() + "v1/scopes/";

        // Test to create a new scope.
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildPost(Entity.json(createScopeRequest)).invoke();
        assertEquals("Create Scope response code", expectedResult, response.getStatus());
        response.close();
    }

    @Override
    @Test
    public void testUpdateStream() throws ExecutionException, InterruptedException {
        String resourceURI = getURI() + "v1/scopes/" + scope1 + "/streams/stream1";

        // Test to update an existing stream
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildPut(Entity.json(updateStreamRequest)).invoke();
        assertEquals("Update Stream Status", expectedResult, response.getStatus());
    }

    @Override
    @Test
    public void testListReaderGroups() {
        final String resourceURI = getURI() + "v1/scopes/scope1/readergroups";
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Reader Groups response code", expectedResult, response.getStatus());
    }

    @Override
    public void testGetReaderGroup() {
        final String resourceURI = getURI() + "v1/scopes/scope1/readergroups/readergroup1";
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Reader Groups response code", expectedResult, response.getStatus());
    }

    @Override
    @Test
    public void testDeleteStream() throws Exception {
        final String resourceURI = getURI() + "v1/scopes/scope1/streams/stream1";

        // Test to delete a sealed stream
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete Stream response code", expectedResult, response.getStatus());
        response.close();
    }

    @Override
    @Test
    public void testGetScalingEvents() throws Exception {
        String resourceURI = getURI() + "v1/scopes/scope1/streams/stream1/scaling-events";
        Response response = addAuthHeaders(client.target(resourceURI).queryParam("from", new Date()).
                queryParam("to", new Date()).request()).buildGet().invoke();
        assertEquals("Get Scaling Events response code", 404, response.getStatus());

    }

    @Override
    @Test
    public void testGetStream() throws ExecutionException, InterruptedException {
        String resourceURI = getURI() + "v1/scopes/" + scope1 + "/streams/stream1";
        String resourceURI2 = getURI() + "v1/scopes/" + scope1 + "/streams/stream2";

        // Test to get an existing stream
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("Get Stream Config Status", expectedResult, response.getStatus());
    }

    @Override
    @Test
    public void testlistScopes() throws ExecutionException, InterruptedException {
        final String resourceURI = getURI() + "v1/scopes";

        // Test to list scopes.
        List<String> scopesList = Arrays.asList("scope1", "scope2");
        when(mockControllerService.listScopes(anyLong())).thenReturn(CompletableFuture.completedFuture(scopesList));
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Scopes response code", expectedResult, response.getStatus());
    }

    @Override
    @Test
    public void testListStreams() throws ExecutionException, InterruptedException {
        final String resourceURI = getURI() + "v1/scopes/scope1/streams";

        Response response = addAuthHeaders(client.target(resourceURI).request()).buildGet().invoke();
        assertEquals("List Streams response code", expectedResult, response.getStatus());
    }

    @Override
    public void testDeleteReaderGroup() {
        final String resourceURI = getURI() + "v1/scopes/scope1/readergroups/readergroup1";
        // Test to delete a reader group
        Response response = addAuthHeaders(client.target(resourceURI).request()).buildDelete().invoke();
        assertEquals("Delete reader group response code", expectedResult, response.getStatus());
        response.close();
    }
}
