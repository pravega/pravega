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
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

/**
 * Unit tests for the password-based auth handler plugin.
 */
public class PasswordAuthHandlerTest {
    private final StrongPasswordProcessor pwdProcessor = StrongPasswordProcessor.builder().build();
    private final Base64.Encoder base64Encoder = Base64.getEncoder();

    //region Tests verifying authentication

    @Test(expected = CompletionException.class)
    public void initializeFailsIfAccountFileMissing() {
        PasswordAuthHandler authHandler = new PasswordAuthHandler();
        authHandler.initialize("nonexistent/accounts/file/path");
    }

    @Test
    public void authenticatesValidUserSuccessfully() {
        String username = "user";
        String password = "password";
        ConcurrentHashMap<String, AccessControlList> aclsByUsers = prepareMapOfAclsByUserName(username, password,
                "prn::*",
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
    public void authorizesSuperUserSpecifiedUsingNewFormat() {
        String username = "test-user";
        String password = "test-password";
        ConcurrentHashMap<String, AccessControlList> aclsByUsers = prepareMapOfAclsByUserName(username, password,
                "prn::*", AuthHandler.Permissions.READ_UPDATE);
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers, false);

        Principal principal = authHandler.authenticate(prepareToken(username, password));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authHandler.authorize("prn::/", principal));
        assertEquals(AuthHandler.Permissions.READ_UPDATE,
                authHandler.authorize("prn::/scope:testscope", principal));
        assertEquals(AuthHandler.Permissions.READ_UPDATE,
                authHandler.authorize("prn::/scope:testscope/stream:teststream", principal));
    }

    @Test
    public void failsAuthorizationWhenUserIsNotAuthorizedToAResource() {
        String username = "test-user";
        String password = "test-password";
        ConcurrentHashMap<String, AccessControlList> aclsByUsers = prepareMapOfAclsByUserName(username, password,
                "prn::/scope:testscope/stream:teststream", AuthHandler.Permissions.READ);
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers, false);

        Principal principal = authHandler.authenticate(prepareToken(username, password));
        assertEquals(AuthHandler.Permissions.READ, authHandler.authorize(
                "prn::/scope:testscope/stream:teststream", principal));
        assertEquals(AuthHandler.Permissions.NONE, authHandler.authorize("prn::/", principal));
        assertEquals(AuthHandler.Permissions.NONE, authHandler.authorize("prn::/testscope", principal));
    }

    @Test
    public void verifyMultiAclUserAuthorization() {
        String username = "test-user";
        String password = "test-password";
        List<String> resources = Arrays.asList("prn::/", "prn::/scope:testscope", "prn::/scope:testscope/stream:teststream");
        List<AuthHandler.Permissions> permissions =
                Arrays.asList(AuthHandler.Permissions.NONE, AuthHandler.Permissions.READ, AuthHandler.Permissions.READ);
        ConcurrentHashMap<String, AccessControlList> aclsByUsers = prepareMapOfAclsByUserName(username, password,
                resources, permissions);
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers, false);
        Principal principal = authHandler.authenticate(prepareToken(username, password));

        assertEquals(AuthHandler.Permissions.READ, authHandler.authorize("prn::/scope:testscope", principal));
        assertEquals(AuthHandler.Permissions.READ, authHandler.authorize("prn::/scope:testscope/stream:teststream", principal));

        assertEquals(AuthHandler.Permissions.NONE, authHandler.authorize("prn::/", principal));
        assertEquals(AuthHandler.Permissions.NONE, authHandler.authorize("prn::/scope:testscope2", principal));
        assertEquals(AuthHandler.Permissions.NONE, authHandler.authorize("prn::/scope:testscope/stream:teststream2", principal));
    }

    @Test
    public void processUserEntrySetsSingleUserEntry() {
        String resourcePattern = "prn::*";
        String line = "test-user:encrypted-password:" + resourcePattern + ",READ_UPDATE";
        PasswordAuthHandler objectUnderTest = new PasswordAuthHandler();

        ConcurrentHashMap<String, AccessControlList> aclsByUser = new ConcurrentHashMap<>();
        objectUnderTest.processUserEntry(line, aclsByUser);

        assertEquals(1, aclsByUser.size());
        AccessControlList acl = aclsByUser.get("test-user");
        assertNotNull(acl);
        assertEquals(1, acl.getEntries().size());

        AccessControlEntry accessControlEntry = acl.getEntries().get(0);
        assertEquals("prn::.*", accessControlEntry.getResourcePattern());
        assertEquals(AuthHandler.Permissions.READ_UPDATE, accessControlEntry.getPermissions());
    }

    @Test
    public void processUserEntrySetsMultipleUserEntries() {
        String resourcePattern1 = "prn::/scope:abc/*";
        String resourcePattern2 = "prn::/scope:def/stream:xyz";
        String resourcePattern3 = "prn::/scope:def/reader-group:rg*";
        String line1 = "test-user1:encrypted-password:" + resourcePattern1 + ",READ_UPDATE";
        String line2 = "test-user2:encrypted-password:" + resourcePattern1 + ",READ_UPDATE;"
                + resourcePattern2 + ",READ_UPDATE;"
                + resourcePattern3 + ",READ;";

        PasswordAuthHandler objectUnderTest = new PasswordAuthHandler();
        ConcurrentHashMap<String, AccessControlList> aclsByUser = new ConcurrentHashMap<>();
        objectUnderTest.processUserEntry(line1, aclsByUser);
        objectUnderTest.processUserEntry(line2, aclsByUser);

        assertEquals(2, aclsByUser.size());
        AccessControlList acl = aclsByUser.get("test-user2");
        List<AccessControlEntry> aces = acl.getEntries();
        assertEquals(3, aces.size());
        assertEquals("prn::/scope:abc/.*", aces.get(0).getResourcePattern());
        assertEquals(resourcePattern2, aces.get(1).getResourcePattern());
        assertEquals("prn::/scope:def/reader-group:rg.*", aces.get(2).getResourcePattern());

        assertEquals(AuthHandler.Permissions.READ_UPDATE, aces.get(0).getPermissions());
        assertEquals(AuthHandler.Permissions.READ_UPDATE, aces.get(1).getPermissions());
        assertEquals(AuthHandler.Permissions.READ, aces.get(2).getPermissions());
    }

    @Test
    public void parseAclReturnsValidInputForSingleValuedEntries() {
        PasswordAuthHandler handler = new PasswordAuthHandler();
        assertSame(1, handler.parseAcl("prn::*,READ_UPDATE").size());
        assertSame(1, handler.parseAcl("prn::/scope:testScope,READ_UPDATE").size());
        assertSame(1, handler.parseAcl("prn::/scope:testScope/stream:testStream,READ_UPDATE").size());
    }

    @Test
    public void parseAclReturnsValidInputForMultiValuedEntries() {
        PasswordAuthHandler handler = new PasswordAuthHandler();

        assertSame(2, handler.parseAcl("prn::/scope:str1;prn::/scope:sc2/stream:str2").size());

        // Ending with `;`
        assertSame(2, handler.parseAcl("prn::/scope:str1;prn::/scope:sc2/stream:str2;").size());
    }

    @Test
    public void parseAclReturnsEmptyListForEmptyEntries() {
        PasswordAuthHandler handler = new PasswordAuthHandler();

        assertSame(0, handler.parseAcl("").size());
        assertSame(0, handler.parseAcl(" ").size());
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
        result.put(user, new AccessControlList(pwdProcessor.encryptPassword(password), aces));
        return result;
    }

    //endregion
}
