/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.auth.AuthConstants;
import io.pravega.auth.AuthException;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.ServerConfig;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PasswordAuthHandler implements AuthHandler {
    private final ConcurrentHashMap<String, PravegaACls> userMap;
    private final StrongPasswordProcessor encryptor;

    public PasswordAuthHandler() {
        userMap = new ConcurrentHashMap<>();
        encryptor = StrongPasswordProcessor.builder().build();
    }

    private void loadPasswordFile(String userPasswordFile) {
        try (FileReader reader = new FileReader(userPasswordFile);
             BufferedReader lineReader = new BufferedReader(reader)) {
            String line;
            while ( !Strings.isNullOrEmpty(line = lineReader.readLine())) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] userFields = line.split(":");
                if (userFields.length >= 2) {
                    String acls;
                    if (userFields.length == 2) {
                        acls = "";
                    } else {
                        acls = userFields[2];
                    }
                    userMap.put(userFields[0], new PravegaACls(userFields[1], getAcls(acls)));
                }
            }
        } catch (IOException e) {
            throw new CompletionException(e);
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
        String password = parts[1];

        try {
            if (userMap.containsKey(userName) && encryptor.checkPassword(password, userMap.get(userName).encryptedPassword)) {
                return new UserPrincipal(userName);
            }
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            log.warn("Exception during password authentication", e);
            throw new AuthenticationException(e);
        }
        throw new AuthenticationException("User authentication exception");
    }

    @Override
    public Permissions authorize(String resource, Principal principal) {
        String userName = principal.getName();

        if (Strings.isNullOrEmpty(userName) || !userMap.containsKey(userName)) {
            throw new CompletionException(new AuthenticationException(userName));
        }
        return authorizeForUser(userMap.get(userName), resource);
    }

    /**
     * This method exists expressly for unit testing purposes. It loads the contents of the specified
     * {@code passwordFile} into this object.
     *
     * @param passwordFile the file with a list of users, their encrypted passwords and their ACLs that is to be used
     *                     by this instance.
     */
    @VisibleForTesting
    void initialize(String passwordFile) {
        loadPasswordFile(passwordFile);
    }

    @Override
    public void initialize(ServerConfig serverConfig) {
        loadPasswordFile(((GRPCServerConfig) serverConfig).getUserPasswordFile());
    }

    private static String[] parseToken(String token) {
        String[] parts = new String(Base64.getDecoder().decode(token), Charsets.UTF_8).split(":", 2);
        Preconditions.checkArgument(parts.length == 2, "Invalid authorization token");
        return parts;
    }

    private Permissions authorizeForUser(PravegaACls pravegaACls, String resource) {
        Permissions result = Permissions.NONE;

        /**
         *  `*` Means a wildcard.
         *  If It is a direct match, return the ACLs.
         *  If it is a partial match, the target has to end with a `/`
         */
        for (PravegaAcl acl : pravegaACls.acls) {

            // Separating into different blocks, to make the code more understandable.
            // It makes the code look a bit strange, but it is still simpler and easier to decipher than what it be
            // if we combine the conditions.

            if (acl.isResource(resource)) {
                // Example: resource = "myscope", acl-resource = "myscope"
                result = acl.permissions;
                break;
            }

            if (acl.isResource("/*") && !resource.contains("/")) {
                // Example: resource = "myscope", acl-resource ="/*"
                result = acl.permissions;
                break;
            }

            if (acl.resourceEndsWith("/") && acl.resourceStartsWith(resource)) {
                result = acl.permissions;
                break;
            }

            // Say, resource is myscope/mystream. ACL specifies permission for myscope/*.
            // Auth should return the the ACL's permissions in that case.
            if (resource.contains("/") && !resource.endsWith("/")) {
                String[] values = resource.split("/");
                if (acl.isResource(values[0] + "/*")) {
                    result = acl.permissions;
                    break;
                }
            }

            if (acl.isResource("*") && acl.hasHigherPermissionsThan(result)) {
                result = acl.permissions;
                break;
            }
        }
        return result;
    }

    private List<PravegaAcl> getAcls(String aclString) {
        return  Arrays.stream(aclString.split(";")).map(acl -> {
            String[] splits = acl.split(",");
            if (splits.length == 0) {
                return null;
            }
            String resource = splits[0];
            String aclVal = "READ";
            if (splits.length >= 2) {
                aclVal = splits[1];

            }
            return new PravegaAcl(resource,
                    Permissions.valueOf(aclVal));
        }).collect(Collectors.toList());
    }

    @Data
    private class PravegaACls {
        private final String encryptedPassword;
        private final List<PravegaAcl> acls;
    }

    @Data
    private class PravegaAcl {
        private final String resourceRepresentation;
        private final Permissions permissions;

        public boolean isResource(String resource) {
            return resourceRepresentation.equals(resource);
        }

        public boolean resourceEndsWith(String resource) {
            return resourceRepresentation.endsWith(resource);
        }

        public boolean resourceStartsWith(String resource) {
            return resourceRepresentation.startsWith(resource);
        }

        public boolean hasHigherPermissionsThan(Permissions input) {
            return this.permissions.ordinal() > input.ordinal();
        }
    }
}
