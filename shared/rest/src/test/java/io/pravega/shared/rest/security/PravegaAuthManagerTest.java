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
package io.pravega.shared.rest.security;

import io.pravega.auth.AuthConstants;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.TestAuthHandler;
import io.pravega.shared.rest.RESTServerConfig;
import io.pravega.shared.rest.impl.RESTServerConfigImpl;
import io.pravega.shared.security.auth.Credentials;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import io.pravega.auth.AuthFileUtils;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.pravega.test.common.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PravegaAuthManagerTest {
    private final static File PWD_AUTH_HANDLER_FILE;
    private final static AuthHandlerManager AUTH_HANDLER_MANAGER;

    static {
        try {
            PWD_AUTH_HANDLER_FILE = File.createTempFile("passwd", ".txt");
            StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();
            try (FileWriter writer = new FileWriter(PWD_AUTH_HANDLER_FILE.getAbsolutePath())) {
                AuthFileUtils.addAuthFileEntry(writer, "#:");
                AuthFileUtils.addAuthFileEntry(writer, ":");
                AuthFileUtils.addAuthFileEntry(writer, "::");
                AuthFileUtils.addAuthFileEntry(writer, ":::");
                AuthFileUtils.addAuthFileEntry(writer, "dummy", "password", new ArrayList<>());
                AuthFileUtils.addAuthFileEntry(writer, "dummy1", "password", Arrays.asList("prn::/scope:readresource;"));
                AuthFileUtils.addAuthFileEntry(writer, "dummy2", "password", Arrays.asList(
                        "prn::/scope:readresource,READ",
                        "prn::/scope:specificresouce,READ",
                        "prn::/scope:totalaccess,READ_UPDATE"));
                AuthFileUtils.addAuthFileEntry(writer, "dummy3", passwordEncryptor.encryptPassword("password"), Arrays.asList(
                        "prn::/scope:readresource,READ",
                        "prn::/scope:specificresouce,READ",
                        "prn::/scope:readresource/*,READ",
                        "prn::/scope:totalaccess,READ_UPDATE"));
                AuthFileUtils.addAuthFileEntry(writer, "dummy4", passwordEncryptor.encryptPassword("password"), Arrays.asList(
                        "prn::/scope:readresource",
                        "prn::/scope:specificresouce,READ",
                        "prn::*,READ_UPDATE"));
            }

            //Test the registration method.
            RESTServerConfig config = RESTServerConfigImpl.builder()
                    .authorizationEnabled(true)
                    .userPasswordFile(PWD_AUTH_HANDLER_FILE.getAbsolutePath())
                    .port(TestUtils.getAvailableListenPort())
                    .host("localhost")
                    .build();

            AUTH_HANDLER_MANAGER = new AuthHandlerManager(config);
            AUTH_HANDLER_MANAGER.registerHandler(new TestAuthHandler());
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

    @BeforeClass
    public static void before() {
        Map<String, AuthHandler> handlers = AUTH_HANDLER_MANAGER.getHandlerMap();
        assertTrue("There should be at least two registered handlers.", handlers.size() >= 2);
        // Make sure that it contains our TestAuthHandler manually registered and the PasswordAuthHandler ('Basic') loaded by the ServiceLoader.
        assertNotNull("'Basic' AuthHandler not found.", handlers.get("Basic"));
        assertNotNull("'testHandler' AuthHandler not found.", handlers.get("testHandler"));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PWD_AUTH_HANDLER_FILE.delete();
    }

    @Test
    public void testMalformedAuthorizationHeaderIsRejected() {
        //Empty authorization header.
        assertThrows(AuthenticationException.class, () ->
                AUTH_HANDLER_MANAGER.authenticateAndAuthorize("prn::/scope:hi", "", AuthHandler.Permissions.READ));

        //Specify a credentials ith valid method but malformed parameters for password interceptor.
        assertThrows(IllegalArgumentException.class, () ->
                AUTH_HANDLER_MANAGER.authenticateAndAuthorize("prn::/scope:hi", credentials(AuthConstants.BASIC, ":"),
                        AuthHandler.Permissions.READ));
    }

    @Test
    public void testUnRegisteredAuthMethodThrowsAuthenticationException() {
        //Non existent interceptor method.
        assertThrows(AuthenticationException.class, () ->
                AUTH_HANDLER_MANAGER.authenticateAndAuthorize("prn::/scope:hi", credentials("invalid", ""), AuthHandler.Permissions.READ));
    }

    @Test
    public void testWrongPasswordThrowsAuthenticationException() {
        //Specify a valid method but incorrect password for password interceptor.
        assertThrows(AuthenticationException.class, () ->
                AUTH_HANDLER_MANAGER.authenticateAndAuthorize("prn::/scope:hi", basic("dummy3", "wrong"), AuthHandler.Permissions.READ));
    }

    @Test
    public void testAuthenticatesUserAndAuthorizesResources() {
        //Valid parameters for default interceptor
        assertTrue("Read access for read resource should return true",
                AUTH_HANDLER_MANAGER.authenticateAndAuthorize("prn::/scope:readresource",
                        basic("dummy3", "password"), AuthHandler.Permissions.READ));
    }

    @Test
    public void testAuthorizationFailsForInvalidOrUnauthorizedResource() {
        //Specify a valid method and parameters but invalid resource for default interceptor.
        assertFalse("Not existent resource should return false",
                AUTH_HANDLER_MANAGER.authenticateAndAuthorize("", basic("dummy3", "password"),
                        AuthHandler.Permissions.READ));

        //Levels of access
        assertFalse("Write access for read resource should return false",
                AUTH_HANDLER_MANAGER.authenticateAndAuthorize("prn::/scope:readresource", basic("dummy3", "password"), AuthHandler.Permissions.READ_UPDATE));
    }

    @Test
    public void testAuthorizationSucceedsForAuthorizedResources() {
        //Valid parameters for default interceptor
        assertTrue("Read access for read resource should return true",
                AUTH_HANDLER_MANAGER.authenticateAndAuthorize("prn::/scope:readresource", basic("dummy3", "password"), AuthHandler.Permissions.READ));

        //Stream/scope access should be extended to segment.
        assertTrue("Read access for read resource should return true",
                AUTH_HANDLER_MANAGER.authenticateAndAuthorize("prn::/scope:readresource/stream:readStream", basic("dummy3", "password"), AuthHandler.Permissions.READ));

        assertTrue("Read access for write resource should return true",
                AUTH_HANDLER_MANAGER.authenticateAndAuthorize("prn::/scope:totalaccess", basic("dummy3", "password"), AuthHandler.Permissions.READ));

        assertTrue("Write access for write resource should return true",
                AUTH_HANDLER_MANAGER.authenticateAndAuthorize("prn::/scope:totalaccess", basic("dummy3", "password"), AuthHandler.Permissions.READ_UPDATE));

        //Check the wildcard access
        assertTrue("Write access for write resource should return true",
                AUTH_HANDLER_MANAGER.authenticateAndAuthorize("prn::/scope:totalaccess", basic("dummy4", "password"), AuthHandler.Permissions.READ_UPDATE));

        assertTrue("Test handler should be called", AUTH_HANDLER_MANAGER.authenticateAndAuthorize("any", testHandler(), AuthHandler.Permissions.READ));
    }

    private static String credentials(String scheme, String token) {
        return scheme + " " + token;
    }

    private static String credentials(Credentials credentials) {
        return credentials(credentials.getAuthenticationType(), credentials.getAuthenticationToken());
    }

    private static String basic(String userName, String password) {
        return credentials(new DefaultCredentials(password, userName));
    }

    private static String testHandler() {
        return credentials("testHandler", "token");
    }
}
