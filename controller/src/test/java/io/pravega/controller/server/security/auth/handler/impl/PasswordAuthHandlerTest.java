/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.security.auth.handler.impl;

import com.google.common.base.Charsets;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import io.pravega.controller.server.security.auth.StrongPasswordProcessor;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.junit.Test;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for the password-based auth handler plugin.
 */
public class PasswordAuthHandlerTest {


    private final StrongPasswordProcessor encrypter = StrongPasswordProcessor.builder().build();
    private final Base64.Encoder base64Encoder = Base64.getEncoder();


    //region Tests verifying authentication

    @Test
    public void authenticatesValidUserSuccessfully() {
        String username = "user";
        String password = "password";
        ConcurrentHashMap<String, AccessControlList> aclsByUsers = prepareMapOfAclsByUserName(username, password, "*",
                AuthHandler.Permissions.READ_UPDATE);
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers, true);

        Principal principal = authHandler.authenticate(prepareToken(username, password));
        assertNotNull(principal);
        assertEquals(username, principal.getName());
    }

    @Test(expected = AuthenticationException.class)
    public void authenticationFailsForUserWithBadCredentials() {
        String username = "user";
        String password = "password";
        ConcurrentHashMap<String, AccessControlList> aclsByUsers = prepareMapOfAclsByUserName(username, password, "*",
                AuthHandler.Permissions.READ_UPDATE);
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers, true);
        authHandler.authenticate(prepareToken(username, "bad-password"));
    }

    @Test(expected = AuthenticationException.class)
    public void authenticationFailsForMissingUser() {
        String username = "user1";
        String password = "password";
        ConcurrentHashMap<String, AccessControlList> aclsByUsers = prepareMapOfAclsByUserName(username, password, "*",
                AuthHandler.Permissions.READ_UPDATE);
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers, true);
        Principal principal = authHandler.authenticate(prepareToken("user-nonexistent", password));
    }

    //endregion Tests verifying authentication

    //region Tests verifying authorization

    @Test
    public void authorizesSuperUserSpecifiedUsingOldFormat() {
        String username = "test-user";
        String password = "test-password";
        ConcurrentHashMap<String, AccessControlList> aclsByUsers = prepareMapOfAclsByUserName(username, password, "*",
                AuthHandler.Permissions.READ_UPDATE);
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers, false);

        Principal principal = authHandler.authenticate(prepareToken(username, password));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authHandler.authorize("/", principal));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authHandler.authorize("testscope", principal));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authHandler.authorize("testscope/teststream", principal));
    }

    @Test
    public void authorizesSuperUserSpecifiedUsingNewFormat() {
        String username = "test-user";
        String password = "test-password";
        ConcurrentHashMap<String, AccessControlList> aclsByUsers = prepareMapOfAclsByUserName(username, password,
                "pravega::*", AuthHandler.Permissions.READ_UPDATE);
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers, true);

        Principal principal = authHandler.authenticate(prepareToken(username, password));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authHandler.authorize("pravega::/", principal));
        assertEquals(AuthHandler.Permissions.READ_UPDATE,
                authHandler.authorize("pravega::/scope:testscope", principal));
        assertEquals(AuthHandler.Permissions.READ_UPDATE,
                authHandler.authorize("pravega::/scope:testscope/stream:teststream", principal));
    }

    @Test
    public void failsAuthorizationWhenUserIsNotAuthorizedToAResource() {
        String username = "test-user";
        String password = "test-password";
        ConcurrentHashMap<String, AccessControlList> aclsByUsers = prepareMapOfAclsByUserName(username, password,
                "testscope/teststream", AuthHandler.Permissions.READ);
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers, false);

        Principal principal = authHandler.authenticate(prepareToken(username, password));
        assertEquals(AuthHandler.Permissions.READ, authHandler.authorize("testscope/teststream", principal));
        assertEquals(AuthHandler.Permissions.NONE, authHandler.authorize("/", principal));
        assertEquals(AuthHandler.Permissions.NONE, authHandler.authorize("testscope", principal));
    }

    @Test
    public void verifyMultiAclUserAuthorization() {
        String username = "test-user";
        String password = "test-password";
        List<String> resources = Arrays.asList("/", "testscope", "testscope/teststream");
        List<AuthHandler.Permissions> permissions =
                Arrays.asList(AuthHandler.Permissions.NONE, AuthHandler.Permissions.READ, AuthHandler.Permissions.READ);
        ConcurrentHashMap<String, AccessControlList> aclsByUsers = prepareMapOfAclsByUserName(username, password,
                resources, permissions);
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers, false);
        Principal principal = authHandler.authenticate(prepareToken(username, password));

        assertEquals(AuthHandler.Permissions.READ, authHandler.authorize("testscope", principal));
        assertEquals(AuthHandler.Permissions.READ, authHandler.authorize("testscope/teststream", principal));

        assertEquals(AuthHandler.Permissions.NONE, authHandler.authorize("/", principal));
        assertEquals(AuthHandler.Permissions.NONE, authHandler.authorize("testscope2", principal));
        assertEquals(AuthHandler.Permissions.NONE, authHandler.authorize("testscope/teststream2", principal));
    }

    //endregion

    //region Private methods

    private String prepareToken(@NonNull String username, @NonNull String password) {
        return base64Encoder.encodeToString(String.format("%s:%s", username, password).getBytes(Charsets.UTF_8));
    }

    @SneakyThrows
    private ConcurrentHashMap<String, AccessControlList> prepareMapOfAclsByUserName(
            @NonNull String user, @NonNull String password,
            @NonNull String resource, @NonNull AuthHandler.Permissions permission)  {
        return prepareMapOfAclsByUserName(user, password, Arrays.asList(resource), Arrays.asList(permission));
    }

    @SneakyThrows
    private ConcurrentHashMap<String, AccessControlList> prepareMapOfAclsByUserName(
            @NonNull String user, @NonNull String password,
            @NonNull List<String> resources, @NonNull List<AuthHandler.Permissions> permissions)  {
        ConcurrentHashMap<String, AccessControlList> result = new ConcurrentHashMap<>();

        List<AccessControlEntry> aces = new ArrayList<>(resources.size());
        for (int i = 0; i < resources.size(); i++) {
            AccessControlEntry ace = new AccessControlEntry(resources.get(i), permissions.get(i));
            aces.add(ace);
        }
        result.put(user, new AccessControlList(encrypter.encryptPassword(password), aces));
        return result;
    }

    //endregion
}
