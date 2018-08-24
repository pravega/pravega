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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.ServerConfig;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PasswordAuthHandler implements AuthHandler {
    private static final String DEFAULT_NAME = "Pravega-Default";
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
        return DEFAULT_NAME;
    }

    @Override
    public boolean authenticate(Map<String, String> headers) {
        String userName = headers.get("username");
        String password = headers.get("password");
        Preconditions.checkArgument(userName != null, "Username not found in header");
        Preconditions.checkArgument(password != null, "Password not found in header");

        try {
            return userMap.containsKey(userName) && encryptor.checkPassword(password, userMap.get(userName).encryptedPassword);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            log.warn("Excpetion during password authentication", e);
            return false;
        }
    }

    @Override
    public Permissions authorize(String resource, Map<String, String> headers) {
        String userName = headers.get("username");
        if (Strings.isNullOrEmpty(userName) || !userMap.containsKey(userName)) {
            throw new CompletionException(new AuthenticationException(userName));
        }
        return authorizeForUser(userMap.get(userName), resource);

    }

    @Override
    public void initialize(ServerConfig serverConfig) {
        loadPasswordFile(((GRPCServerConfig) serverConfig).getUserPasswordFile());
    }

    private Permissions authorizeForUser(PravegaACls pravegaACls, String resource) {
        Permissions retVal = Permissions.NONE;

        /**
         *  `*` Means a wildcard.
         *  If It is a direct match, return the ACLs.
         *  If it is a partial match, the target has to end with a `/`
         */
        for (PravegaAcl acl : pravegaACls.acls) {
            if (acl.resource.equals(resource) ||
                    (acl.resource.endsWith("/") && resource.startsWith(acl.resource))
                    || (resource.startsWith(acl.resource + "/"))
                    || ((acl.resource.equals("*")) && (acl.acl.ordinal() > retVal.ordinal()))) {
                retVal = acl.acl;
            }
        }
        return retVal;
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
        private final String resource;
        private final Permissions acl;


    }
}
