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
package io.pravega.client;

import io.pravega.shared.security.auth.Credentials;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class CredentialsExtractorTest {

    @Test
    public void testExtractsCredentialsFromProperties() {
        Properties properties = new Properties();
        properties.setProperty("pravega.client.auth.method", "amethod");
        properties.setProperty("pravega.client.auth.token", "atoken");

        ClientConfig clientConfig = ClientConfig.builder().extractCredentials(properties, null).build();
        Credentials credentials = clientConfig.getCredentials();

        assertNotNull(credentials);
        assertNotNull("io.pravega.client.ClientConfig$ClientConfigBuilder$1",
                credentials.getClass());
        assertEquals("amethod", credentials.getAuthenticationType());
        assertEquals("atoken", credentials.getAuthenticationToken());
    }

    @Test
    public void testExtractsCredentialsFromEnvVariables() {
        Map<String, String> authEnvVariables = new HashMap<>();
        authEnvVariables.put("pravega_client_auth_method", "amethod");
        authEnvVariables.put("pravega_client_auth_token", "atoken");

        ClientConfig clientConfig = ClientConfig.builder().extractCredentials(null, authEnvVariables).build();
        Credentials credentials = clientConfig.getCredentials();

        assertNotNull(credentials);
        assertNotNull("io.pravega.client.ClientConfig$ClientConfigBuilder$1",
                credentials.getClass());
        assertEquals("amethod", credentials.getAuthenticationType());
        assertEquals("atoken", credentials.getAuthenticationToken());
    }

    @Test
    public void testExplicitlySpecifiedCredentialsAreNotOverridden() {
        Properties properties = new Properties();
        properties.setProperty("pravega.client.auth.method", "amethod");
        properties.setProperty("pravega.client.auth.token", "atoken");

        Map<String, String> authEnvVariables = new HashMap<>();
        authEnvVariables.put("pravega_client_auth_method", "amethod");
        authEnvVariables.put("pravega_client_auth_token", "atoken");

        ClientConfig clientConfig = ClientConfig.builder()
                .credentials(new Credentials() {
                    @Override
                    public String getAuthenticationType() {
                        return "typeSpecifiedViaExplicitObject";
                    }

                    @Override
                    public String getAuthenticationToken() {
                        return "tokenSpecifiedViaExplicitObject";
                    }
                }).extractCredentials(properties, authEnvVariables)
                .build();

        assertEquals("Explicitly set credentials should not be overridden", "typeSpecifiedViaExplicitObject",
                clientConfig.getCredentials().getAuthenticationType());

        assertEquals("Explicitly set credentials should not be overridden", "tokenSpecifiedViaExplicitObject",
                clientConfig.getCredentials().getAuthenticationToken());
    }

    @Test
    public void testCredentialsSpecifiedViaPropertiesAreNotOverriddenByEnvVariables() {
        Properties properties = new Properties();
        properties.setProperty("pravega.client.auth.method", "amethod");
        properties.setProperty("pravega.client.auth.token", "atoken");

        Map<String, String> authEnvVariables = new HashMap<>();
        authEnvVariables.put("pravega_client_auth_method", "bmethod");
        authEnvVariables.put("pravega_client_auth_token", "btoken");

        ClientConfig clientConfig = ClientConfig.builder()
                .extractCredentials(properties, authEnvVariables)
              .build();

        assertEquals("amethod", clientConfig.getCredentials().getAuthenticationType());
        assertEquals("atoken", clientConfig.getCredentials().getAuthenticationToken());
    }

    @Test
    public void testLoadsCredentialsObjOfAGenericTypeFromPropertiesIfLoadDynamicIsFalse() {
        Properties properties = new Properties();
        properties.setProperty("pravega.client.auth.loadDynamic", "false");
        properties.setProperty("pravega.client.auth.method", "amethod");
        properties.setProperty("pravega.client.auth.token", "atoken");

        ClientConfig clientConfig = ClientConfig.builder().extractCredentials(properties, null).build();
        Credentials credentials = clientConfig.getCredentials();

        assertNotNull(credentials);
        assertNotNull("io.pravega.client.ClientConfig$ClientConfigBuilder$1",
                credentials.getClass());
        assertEquals("amethod", credentials.getAuthenticationType());
        assertEquals("atoken", credentials.getAuthenticationToken());
    }

    @Test
    public void testLoadsCredentialsObjOfAGenericTypeFromEnvVariablesIfLoadDynamicIsFalse() {
        Map<String, String> authEnvVariables = new HashMap<>();
        authEnvVariables.put("pravega_client_auth_loadDynamic", "false");
        authEnvVariables.put("pravega_client_auth_method", "amethod");
        authEnvVariables.put("pravega_client_auth_token", "atoken");

        ClientConfig clientConfig =
                ClientConfig.builder().extractCredentials(null, authEnvVariables).build();
        Credentials credentials = clientConfig.getCredentials();

        assertNotNull(credentials);
        assertNotNull("io.pravega.client.ClientConfig$ClientConfigBuilder$1",
                credentials.getClass());
        assertEquals("amethod", credentials.getAuthenticationType());
        assertEquals("atoken", credentials.getAuthenticationToken());
    }

    @Test
    public void testDoesNotLoadCredentialsOfNonExistentClassIfLoadDynamicIsTrue() {
        Properties properties = new Properties();
        properties.setProperty("pravega.client.auth.loadDynamic", "true");
        properties.setProperty("pravega.client.auth.method", "amethod");
        properties.setProperty("pravega.client.auth.token", "atoken");

        Map<String, String> authEnvVariables = new HashMap<>();
        authEnvVariables.put("pravega_client_auth_loadDynamic", "true");
        authEnvVariables.put("pravega_client_auth_method", "amethod");
        authEnvVariables.put("pravega_client_auth_token", "atoken");

        ClientConfig clientConfig = ClientConfig.builder()
                    .extractCredentials(properties, authEnvVariables)
                .build();

        // Expecting a null because there is no Credentials implementation in the classpath that registers an
        // authentication type "amethod".
        assertNull(clientConfig.getCredentials());
    }

    @Test
    public void testLoadsCredentialsObjOfARegisteredTypeFromPropertiesIfLoadDynamicIsTrue() {
        Properties properties = new Properties();
        properties.setProperty("pravega.client.auth.loadDynamic", "true");
        properties.setProperty("pravega.client.auth.method", "Bearer");

        ClientConfig clientConfig = ClientConfig.builder().extractCredentials(properties, null).build();
        Credentials credentials = clientConfig.getCredentials();

        assertNotNull("Credentials is null", credentials);
        assertNotNull(DynamicallyLoadedCreds.class.getName(), credentials.getClass());
        assertEquals("Expected a different authentication type", "Bearer",
                credentials.getAuthenticationType());
    }

    @Test
    public void testLoadsCredentialsObjOfARegisteredTypeFromEnvVariablesIfLoadDynamicIsTrue() {
        Map<String, String> authEnvVariables = new HashMap<>();
        authEnvVariables.put("pravega_client_auth_loadDynamic", "true");
        authEnvVariables.put("pravega_client_auth_method", "Bearer");

        ClientConfig clientConfig = ClientConfig.builder()
                   .extractCredentials(null, authEnvVariables)
                .build();
        Credentials credentials = clientConfig.getCredentials();

        assertNotNull("Credentials is null", credentials);
        assertNotNull(DynamicallyLoadedCreds.class.getName(), credentials.getClass());
        assertEquals("Expected a different authentication type", "Bearer",
                credentials.getAuthenticationType());
    }

    /**
     * The intent of this test is to verify whether an existing Credentials implementation works if the
     * service definition is made using the new interface: `META-INF/services/io.pravega.shared.security.auth.Credentials`.
     * In effect, it verifies that the existing plugin works with a modified service definition based on the new
     * interface.
     */
    @Test
    public void testLoadsLegacyCredentialsUsingNewInterfacePackage() {
        Map<String, String> authEnvVariables = new HashMap<>();
        authEnvVariables.put("pravega_client_auth_loadDynamic", "true");
        authEnvVariables.put("pravega_client_auth_method", LegacyCredentials1.AUTHENTICATION_METHOD);

        ClientConfig clientConfig = ClientConfig.builder()
                .extractCredentials(null, authEnvVariables)
                .build();
        Credentials credentials = clientConfig.getCredentials();

        assertNotNull("Credentials is null", credentials);
        assertNotNull(LegacyCredentials1.class.getName(), credentials.getClass());
        assertEquals("Expected a different authentication type", LegacyCredentials1.AUTHENTICATION_METHOD,
                credentials.getAuthenticationType());
    }

    /**
     * The intent of this test is to verify whether an existing Credentials implementation works if the
     * service definition is via `META-INF/services/io.pravega.client.stream.impl.Credentials` file.
     * In effect, it verifies that the existing plugin works as-is with the new client.
     */
    @Test
    public void testLoadsLegacyCredentialsUsingOldInterfacePackage() {
        Map<String, String> authEnvVariables = new HashMap<>();
        authEnvVariables.put("pravega_client_auth_loadDynamic", "true");
        authEnvVariables.put("pravega_client_auth_method",
                LegacyCredentials2.AUTHENTICATION_METHOD);

        ClientConfig clientConfig = ClientConfig.builder()
                .extractCredentials(null, authEnvVariables)
                .build();
        Credentials credentials = clientConfig.getCredentials();

        assertNotNull("Credentials is null", credentials);
        assertNotNull(LegacyCredentials2.class.getName(), credentials.getClass());
        assertEquals("Expected a different authentication type",
                LegacyCredentials2.AUTHENTICATION_METHOD, credentials.getAuthenticationType());
    }

    /**
     * A class representing Credentials. It is dynamically loaded using a {@link java.util.ServiceLoader} by
     * the code under test, in the enclosing test class. For ServiceLoader to find it, it is configured in
     * META-INF/services/io.pravega.shared.security.auth.Credentials.
     */
    public static class DynamicallyLoadedCreds implements Credentials {

        @Override
        public String getAuthenticationType() {
            return "Bearer";
        }

        @Override
        public String getAuthenticationToken() {
            return "SomeToken";
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

    @SuppressWarnings("deprecation")
    public static class LegacyCredentials1 implements io.pravega.client.stream.impl.Credentials {
        private static final String TOKEN = "custom-token-legacy";
        private static final String AUTHENTICATION_METHOD = "custom-method-legacy";

        @Override
        public String getAuthenticationType() {
            return AUTHENTICATION_METHOD;
        }

        @Override
        public String getAuthenticationToken() {
            return TOKEN;
        }
    }

    /**
     * This implementation looks the same as {@link LegacyCredentials1}. But, these two are loaded differently.
     * See how they are loaded differently in the corresponding service definition files under
     * resources/META-INF/services.
     */
    @SuppressWarnings("deprecation")
    public static class LegacyCredentials2 implements io.pravega.client.stream.impl.Credentials {
        private static final String TOKEN = "custom-token-legacy-2";
        private static final String AUTHENTICATION_METHOD = "custom-method-legacy-2";

        @Override
        public String getAuthenticationType() {
            return AUTHENTICATION_METHOD;
        }

        @Override
        public String getAuthenticationToken() {
            return TOKEN;
        }
    }
}