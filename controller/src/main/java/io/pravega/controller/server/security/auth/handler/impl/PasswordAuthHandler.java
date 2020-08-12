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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.auth.AuthConstants;
import io.pravega.auth.AuthException;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.ServerConfig;
import io.pravega.controller.server.security.auth.StrongPasswordProcessor;
import io.pravega.controller.server.security.auth.UserPrincipal;
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
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PasswordAuthHandler implements AuthHandler {
    private final ConcurrentHashMap<String, AccessControlList> userMap;
    private final StrongPasswordProcessor encryptor;
    private final boolean isOldAclFormatEnabled;

    public PasswordAuthHandler() {
        this(new ConcurrentHashMap<>(), false);
    }

    @VisibleForTesting
    PasswordAuthHandler(ConcurrentHashMap<String, AccessControlList> aclByUser, boolean useAclsInOldFormat) {
        userMap = aclByUser;
        encryptor = StrongPasswordProcessor.builder().build();
        isOldAclFormatEnabled = useAclsInOldFormat;
    }

    private void loadPasswordFile(String userPasswordFile) {
        log.debug("Loading {}", userPasswordFile);

        try (FileReader reader = new FileReader(userPasswordFile);
             BufferedReader lineReader = new BufferedReader(reader)) {
            String line;
            while ( !Strings.isNullOrEmpty(line = lineReader.readLine())) {
                if (line.startsWith("#")) { // A commented line
                    continue;
                }
                processUserEntry(line, this.userMap);
            }
        } catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    @VisibleForTesting
    void processUserEntry(String line, ConcurrentHashMap<String, AccessControlList> aclsByUser) {
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
            aclsByUser.put(userFields[0], new AccessControlList(userFields[1], parseAcl(acls)));
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
            if (userMap.containsKey(userName) &&
                    encryptor.checkPassword(password, userMap.get(userName).getEncryptedPassword())) {
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
    public void initialize(String passwordFile) {
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

    private Permissions authorizeForUser(AccessControlList accessControlList, String resource) {
        AclAuthorizer aclAuthorizer = this.isOldAclFormatEnabled ? AclAuthorizer.legacyAuthorizerInstance() :
                AclAuthorizer.instance();
        return aclAuthorizer.authorize(accessControlList, resource);
    }

    private List<AccessControlEntry> parseAcl(String aclString) {
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
            return new AccessControlEntry(resource,
                    Permissions.valueOf(aclVal));
        }).collect(Collectors.toList());
    }
}



