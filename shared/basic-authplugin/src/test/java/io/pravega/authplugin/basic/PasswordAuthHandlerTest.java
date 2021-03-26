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
package io.pravega.authplugin.basic;

import com.google.common.base.Charsets;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthPluginConfig;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.ServerConfig;
import io.pravega.shared.security.auth.PasswordAuthHandlerInput;
import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

import io.pravega.test.common.AssertExtensions;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the password-based auth handler plugin.
 */
public class PasswordAuthHandlerTest {
    private final StrongPasswordProcessor pwdProcessor = StrongPasswordProcessor.builder().build();
    private final Base64.Encoder base64Encoder = Base64.getEncoder();

    //region Tests verifying authentication

    @Test
    public void initializeFailsIfPropertiesIsNull() {
        PasswordAuthHandler objectUnderTest = new PasswordAuthHandler();

        AssertExtensions.assertThrows(NullPointerException.class, () -> {
            // Declaration is necessary to avoid ambiguity of the following call.
            Properties props = null;
            objectUnderTest.initialize(props);
        });

        AssertExtensions.assertThrows(NullPointerException.class, () -> {
            // Declaration is necessary to avoid ambiguity of the following call.
            ServerConfig serverConfig = null;
            objectUnderTest.initialize(serverConfig);
        });
    }

    @Test(expected = RuntimeException.class)
    public void initializeFailsIfAccountFileMissing() {
        PasswordAuthHandler objectUnderTest = new PasswordAuthHandler();
        objectUnderTest.initialize(new Properties());
    }

    @Test(expected = CompletionException.class)
    public void initializeFailsIfAccountFileConfigMissing() {
        PasswordAuthHandler authHandler = new PasswordAuthHandler();

        ServerConfig serverConfig = new ServerConfig() {
            @Override
            public Properties toAuthHandlerProperties() {
                Properties props = new Properties();
                props.setProperty(AuthPluginConfig.BASIC_AUTHPLUGIN_DATABASE, "/random/file");
                return props;
            }
        };
        authHandler.initialize(serverConfig);
    }

    @SneakyThrows
    @Test
    public void initializeWorksWithValidAclsAndComments() {
        PasswordAuthHandler authHandler = new PasswordAuthHandler();

        try (PasswordAuthHandlerInput pwdInputFile = new PasswordAuthHandlerInput("PasswordAuthHandlerTest.init",
                ".txt")) {
            String encryptedPassword = StrongPasswordProcessor.builder().build().encryptPassword("some_password");

            List<PasswordAuthHandlerInput.Entry> entries = Arrays.asList(
                    PasswordAuthHandlerInput.Entry.of("admin", encryptedPassword, "prn::*,READ_UPDATE;"),
                    PasswordAuthHandlerInput.Entry.of("appaccount", encryptedPassword, "prn::/scope:scope1,READ_UPDATE;"),
                    PasswordAuthHandlerInput.Entry.of("#commented", encryptedPassword, "prn::")
            );
            pwdInputFile.postEntries(entries);

            authHandler.initialize(new ServerConfig() {
                @Override
                public Properties toAuthHandlerProperties() {
                    Properties props = new Properties();
                    props.setProperty(AuthPluginConfig.BASIC_AUTHPLUGIN_DATABASE, pwdInputFile.getFile().getAbsolutePath());
                    return props;
                }
            });

            ConcurrentHashMap<String, AccessControlList> aclsByUser = authHandler.getAclsByUser();

            assertEquals(2, aclsByUser.size());
            assertTrue(aclsByUser.containsKey("admin"));
            assertEquals("prn::/scope:scope1", aclsByUser.get("appaccount").getEntries().get(0).getResourcePattern());
            assertFalse(aclsByUser.containsKey("unauthorizeduser"));
        }
    }

    @Test
    public void authenticatesValidUserSuccessfully() {
        String username = "user";
        String password = "password";
        ConcurrentHashMap<String, AccessControlList> aclsByUsers = prepareMapOfAclsByUserName(username, password,
                "prn::*",
                AuthHandler.Permissions.READ_UPDATE);
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers);

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
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers);
        authHandler.authenticate(prepareToken(username, "bad-password"));
    }

    @Test(expected = AuthenticationException.class)
    public void authenticationFailsForMissingUser() {
        String username = "user1";
        String password = "password";
        ConcurrentHashMap<String, AccessControlList> aclsByUsers = prepareMapOfAclsByUserName(username, password, "*",
                AuthHandler.Permissions.READ_UPDATE);
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers);
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
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers);

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
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers);

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
        PasswordAuthHandler authHandler = new PasswordAuthHandler(aclsByUsers);
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
