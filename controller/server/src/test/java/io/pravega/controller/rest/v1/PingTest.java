/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

import io.pravega.controller.server.rest.resources.PingImpl;
import io.pravega.test.common.TestUtils;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Test;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;

/**
 * Test for ping API.
 */
public class PingTest extends JerseyTest {

    @Override
    protected Application configure() {
        this.forceSet(TestProperties.CONTAINER_PORT, String.valueOf(TestUtils.getAvailableListenPort()));
        return new ResourceConfig(PingImpl.class);
    }

    @Test
    public void test() {
        final Response hello = target("/ping").request().get(Response.class);
        assertEquals(200, hello.getStatus());
    }
}
