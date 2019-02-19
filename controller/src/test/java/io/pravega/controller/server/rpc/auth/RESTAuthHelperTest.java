/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.auth;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static io.pravega.auth.AuthHandler.Permissions.*;

import io.pravega.auth.AuthException;
import io.pravega.controller.mocks.FakeAuthHandler;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

/**
 * Unit tests for the RESTAuthHelper class.
 */
public class RESTAuthHelperTest {

    private RESTAuthHelper authHelper;

    @Before
    public void init() {
        PravegaAuthManager authManager = new PravegaAuthManager(null);
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
        RESTAuthHelper sut = new RESTAuthHelper(new PravegaAuthManager(null));
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
