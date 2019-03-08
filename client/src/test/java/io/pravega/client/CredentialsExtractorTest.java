/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client;

import io.pravega.client.stream.impl.Credentials;
import java.util.HashMap;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class CredentialsExtractorTest {
    @Test
    public void testextractCredentials() {
        //No creds defined
        ClientConfig config = ClientConfig.builder().build();
        assertEquals("Empty list should return null", config.getCredentials(), null);

        //Test custom creds
        Properties properties = new Properties();
        properties.setProperty("pravega.client.auth.method", "temp");
        properties.setProperty("pravega.client.auth.token", "mytoken");

        config = ClientConfig.builder().extractCredentials(properties, new HashMap<String, String>()).build();

        assertEquals("Method is not picked up from properties",
                "temp", config.getCredentials().getAuthenticationType());

        assertEquals("Token is not same",
                "mytoken", config.getCredentials().getAuthenticationToken());

        //If a credential is explicitly mentioned, do not override from properties
        config = ClientConfig.builder().credentials(new Credentials() {
            @Override
            public String getAuthenticationType() {
                return null;
            }

            @Override
            public String getAuthenticationToken() {
                return null;
            }
        }).build();

        config = config.toBuilder().extractCredentials(properties, new HashMap<String, String>()).build();

        assertNotEquals("Credentials should not be overridden",
                config.getCredentials().getAuthenticationType(), "temp");

        //In case dynamic creds system property is false, load the creds from properties
        properties.setProperty("pravega.client.auth.loadDynamic", "false");

        config = ClientConfig.builder().extractCredentials(properties, new HashMap<String, String>()).build();
        assertEquals("Method is not picked up from properties",
                config.getCredentials().getAuthenticationType(), "temp");

        //In case dynamic creds system property is true and class does not exist, the API should return null.
        properties.setProperty("pravega.client.auth.loadDynamic", "true");

        config = ClientConfig.builder().extractCredentials(properties, new HashMap<String, String>()).build();
        Assert.assertNull("Creds should not be picked up from properties",
                config.getCredentials());

        //In case dynamic creds system property is true, the correct class should be loaded.
        properties.setProperty("pravega.client.auth.method", "DynamicallyLoadedCredsSecond");
        config = ClientConfig.builder().extractCredentials(properties, new HashMap<String, String>()).build();
        Assert.assertEquals("Correct creds object should be loaded dynamically",
                config.getCredentials().getAuthenticationType(), "DynamicallyLoadedCredsSecond");
    }

    public static class DynamicallyLoadedCreds implements Credentials {

        @Override
        public String getAuthenticationType() {
            return "DynamicallyLoadedCreds";
        }

        @Override
        public String getAuthenticationToken() {
            return "DynamicallyLoadedCreds";
        }
    }

    public static class DynamicallyLoadedCredsSecond implements Credentials {

        @Override
        public String getAuthenticationType() {
            return "DynamicallyLoadedCredsSecond";
        }

        @Override
        public String getAuthenticationToken() {
            return "DynamicallyLoadedCredsSecond";
        }
    }
}