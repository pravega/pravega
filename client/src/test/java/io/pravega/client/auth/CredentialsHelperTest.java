/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.auth;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.impl.Credentials;
import io.pravega.test.common.AssertExtensions;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class CredentialsHelperTest {
    @Test
    public void testextractCredentials() {
        //No creds defined
        ClientConfig config = ClientConfig.builder().build();
        config = CredentialsHelper.extractCredentials(config);
        assertEquals("Empty list should return null", config.getCredentials(), null);

        //Test custom creds
        System.setProperty("pravega.client.auth.method", "temp");
        System.setProperty("pravega.client.auth.prop1", "prop1");
        System.setProperty("pravega.client.auth.prop2", "prop2");

        config = ClientConfig.builder().build();
        config = CredentialsHelper.extractCredentials(config);

        assertEquals("Method is not picked up from properties",
                config.getCredentials().getAuthenticationType(), "temp");

        AssertExtensions.assertMapEquals("Paramters are not same",
                config.getCredentials().getAuthParameters(),
                ImmutableMap.of("pravega.client.auth.prop1", "prop1",
                        "pravega.client.auth.prop2", "prop2",
                        "pravega.client.auth.method", "temp"));

        //If a credential is explicitly mentioned, do not override from properties
        config = ClientConfig.builder().credentials(new Credentials() {
            @Override
            public String getAuthenticationType() {
                return null;
            }

            @Override
            public Map<String, String> getAuthParameters() {
                return null;
            }
        }).build();

    config = CredentialsHelper.extractCredentials(config);

    assertNotEquals("Credentials should not be overridden",
            config.getCredentials().getAuthenticationType(), "temp");

        //Test custom creds
        System.clearProperty("pravega.client.auth.method");
        System.clearProperty("pravega.client.auth.prop1");
        System.clearProperty("pravega.client.auth.prop2");
    }
}