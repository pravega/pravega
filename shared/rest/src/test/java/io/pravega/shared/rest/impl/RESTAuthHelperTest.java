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
package io.pravega.shared.rest.impl;

import io.pravega.auth.AuthException;
import io.pravega.shared.rest.RESTServerConfig;
import io.pravega.auth.FakeAuthHandler;
import io.pravega.shared.rest.security.RESTAuthHelper;
import io.pravega.shared.rest.security.AuthHandlerManager;
import io.pravega.shared.security.auth.UserPrincipal;
import io.pravega.test.common.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static io.pravega.auth.AuthHandler.Permissions.READ;
import static io.pravega.auth.AuthHandler.Permissions.READ_UPDATE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the RESTAuthHelper class.
 */
public class RESTAuthHelperTest {

    private RESTAuthHelper authHelper;

    @Before
    public void init() {
        RESTServerConfig config = RESTServerConfigImpl.builder()
                .host("localhost")
                .port(TestUtils.getAvailableListenPort())
                .authorizationEnabled(true)
                .userPasswordFile("passwd")
                .tlsEnabled(false)
                .build();

        AuthHandlerManager authManager = new AuthHandlerManager(config);
        authManager.registerHandler(new FakeAuthHandler());
        authHelper = new RESTAuthHelper(authManager);
    }

    @Test
    public void testAuthOkWhenAuthMgrIsNull() throws AuthException {
        RESTAuthHelper authHelper = new RESTAuthHelper(null);
        assertTrue(authHelper.isAuthorized(null, null, null, null));

        authHelper.authenticate(null);
        authHelper.authorize(null, null, null, null);
        authHelper.authenticateAuthorize(null, null, null);
    }

    @Test
    public void testAuthOkForPrivilegedUserWhenCredentialsAreValid() throws AuthException {
        String username = FakeAuthHandler.PRIVILEGED_USER;
        String password = "whatever";

        assertTrue(authHelper.isAuthorized(createAuthHeader(username, password), "/",
                new UserPrincipal(username),  READ_UPDATE));
    }

    @Test
    public void testAuthFailsForUnknownUser() throws AuthException {
        String username = "unknown";
        String password = "whatever";

        boolean authorized = authHelper.isAuthorized(
                createAuthHeader(username, password),
                "/",
                new UserPrincipal(username),
                READ);
        assertFalse(authorized);
    }

    @Test
    public void testAuthOkForUnprivilegedUserForAssignedPermission() throws AuthException {
        String username = FakeAuthHandler.UNPRIVILEGED_USER;
        String password = "whatever";

        boolean authorized = authHelper.isAuthorized(
                createAuthHeader(username, password),
                "/",
                new UserPrincipal(username),
                READ);
        assertFalse(authorized);
    }

    @Test
    public void testAuthFailsForUnprivilegedUserForUnassignedPermission() throws AuthException {
        String username = FakeAuthHandler.UNPRIVILEGED_USER;
        String password = "whatever";

        boolean authorized = authHelper.isAuthorized(
                createAuthHeader(username, password),
                "/",
                new UserPrincipal(username),
                READ_UPDATE);
        assertFalse(authorized);
    }

    @Test
    public void testAuthIsEnabledWhenPravegaAuthManagerIsNonNull() {
        RESTAuthHelper sut = new RESTAuthHelper(new AuthHandlerManager(null));
        assertTrue(sut.isAuthEnabled());
    }

    @Test
    public void testAuthIsDisabledWhenPravegaAuthManagerIsNull() {
        RESTAuthHelper sut = new RESTAuthHelper(null);
        assertFalse(sut.isAuthEnabled());
    }

    private List<String> createAuthHeader(String username, String password)  {
        String value = String.format("%s:%s", username, password);
        String encodedCredentials = null;
        try {
            encodedCredentials = Base64.getEncoder().encodeToString(value.getBytes("utf-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return Arrays.asList("Basic " + encodedCredentials);
    }
}
