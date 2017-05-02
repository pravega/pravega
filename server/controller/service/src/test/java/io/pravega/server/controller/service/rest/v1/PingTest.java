/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.rest.v1;

import io.pravega.server.controller.service.server.rest.resources.PingImpl;
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
