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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.auth.AuthConstants;
import io.pravega.auth.AuthException;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthPluginConfig;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.ServerConfig;
import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import io.pravega.shared.security.auth.UserPrincipal;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PasswordAuthHandler implements AuthHandler {
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final ConcurrentHashMap<String, AccessControlList> aclsByUser;
    private final StrongPasswordProcessor encryptor;

    public PasswordAuthHandler() {
        this(new ConcurrentHashMap<String, AccessControlList>());
    }

    @VisibleForTesting
    PasswordAuthHandler(ConcurrentHashMap<String, AccessControlList> aclByUser) {
        aclsByUser = aclByUser;
        encryptor = StrongPasswordProcessor.builder().build();
    }

    private void loadPasswordFile(String userPasswordFile) {
        log.info("Loading {}", userPasswordFile);

        try (FileReader reader = new FileReader(userPasswordFile);
            BufferedReader lineReader = new BufferedReader(reader)) {
            String line;
            while ( !Strings.isNullOrEmpty(line = lineReader.readLine())) {
                if (line.startsWith("#")) { // A commented line
                    continue;
                }
                processUserEntry(line, this.aclsByUser);
            }
        } catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    @VisibleForTesting
    void processUserEntry(String line, ConcurrentHashMap<String, AccessControlList> userMap) {
        // An entry'd look like this:
        //      testUserName:testEncryptedPassword:prn::/scope:testScope,READ_UPDATE;prn::/scope:testScope/stream:testStream;";

        // Extract the username, encrypted password and the ACL (3 items)
        String[] userFields = line.split(":", 3);

        if (userFields.length >= 2) {
            String acls;
            if (userFields.length == 2) {
                acls = "";
            } else {
                acls = userFields[2];
            }
            userMap.put(userFields[0], new AccessControlList(userFields[1], parseAcl(acls)));
        }
    }

    @Override
    public String getHandlerName() {
        return AuthConstants.BASIC;
    }

    @Override
    public Principal authenticate(String token) throws AuthException {
        String[] parts = parseToken(token);
        String userName = parts[0];
        char[] password = parts[1].toCharArray();

        try {
            if (aclsByUser.containsKey(userName) &&
                    encryptor.checkPassword(password, aclsByUser.get(userName).getEncryptedPassword())) {
                return new UserPrincipal(userName);
            }
            throw new AuthenticationException("User authentication exception");
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            log.warn("Exception during password authentication", e);
            throw new AuthenticationException(e);
        } finally {
            Arrays.fill(password, '0'); // Zero out the password for security.
        }
    }

    @Override
    public Permissions authorize(String resource, Principal principal) {
        String userName = principal.getName();
        if (Strings.isNullOrEmpty(userName) || !aclsByUser.containsKey(userName)) {
            throw new CompletionException(new AuthenticationException(userName));
        }
        return authorizeForUser(aclsByUser.get(userName), resource);
    }

    @Override
    public void initialize(@NonNull ServerConfig config) {
        initialize(config.toAuthHandlerProperties());
    }

    @VisibleForTesting
    void initialize(@NonNull Properties properties) {
        String userAccountsDatabaseFilePath = properties.getProperty(AuthPluginConfig.BASIC_AUTHPLUGIN_DATABASE);
        if (userAccountsDatabaseFilePath == null) {
            throw new RuntimeException("User account database config was absent");
        }
        initialize(userAccountsDatabaseFilePath);
    }

    /**
     * This method exists expressly for unit testing purposes. It loads the contents of the specified
     * {@code passwordFile} into this object.
     *
     * @param passwordFile the file with a list of users, their encrypted passwords and their ACLs that is to be used
     *                     by this instance.
     */
    @VisibleForTesting
    public void initialize(String passwordFile) {
        loadPasswordFile(passwordFile);
    }

    private static String[] parseToken(String token) {
        String[] parts = new String(Base64.getDecoder().decode(token), Charsets.UTF_8).split(":", 2);
        Preconditions.checkArgument(parts.length == 2, "Invalid authorization token");
        return parts;
    }

    private Permissions authorizeForUser(AccessControlList accessControlList, String resource) {
        AclAuthorizer aclAuthorizer = AclAuthorizer.instance();
        return aclAuthorizer.authorize(accessControlList, resource);
    }

    @VisibleForTesting
    List<AccessControlEntry> parseAcl(String aclString) {
        if (aclString == null || aclString.trim().equals("")) {
            return new ArrayList<>();
        }

        // Sample ACL strings:
        //     prn::*,READ_UPDATE (single-valued)
        //     prn::/scope:str1;prn::/scope:sc2/stream:str2; (multi-valued ending with `;`)
        //     prn::/scope:str1;prn::/scope:sc2/stream:str2 (multi-valued)
        return Arrays.stream(aclString.trim().split(";"))
                .map(ace -> AccessControlEntry.fromString(ace))
                .collect(Collectors.toList());
    }
}



