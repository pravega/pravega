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
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Data;
import org.jasypt.util.password.StrongPasswordEncryptor;

public class PravegaDefaultAuthHandler implements PravegaAuthHandler {
    private static final String DEFAULT_NAME = "Pravega-Default";
    private final Map<String, PravegaACls> userMap;

    PravegaDefaultAuthHandler(String userPasswdFile) {
        userMap = new ConcurrentHashMap<>();
        loadPasswdFile(userPasswdFile);
    }

    //TODO: Add tests for wrong file
    private void loadPasswdFile(String userPasswdFile) {
        try (FileReader reader = new FileReader(userPasswdFile);
             BufferedReader lineReader = new BufferedReader(reader)
        ) {
            String line = lineReader.readLine();
            while ( !Strings.isNullOrEmpty(line)) {
                if (!Strings.isNullOrEmpty(line) && !line.startsWith("#")) {
                    String[] userFields = line.split(":");
                    if (userFields.length >= 2) {
                        userMap.put(userFields[0], new PravegaACls(userFields[1], getAcls(userFields[2])));
                    }
                }
                line = lineReader.readLine();
            }
        } catch (Exception e) {
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
        String passwd = headers.get("password");
        Preconditions.checkNotNull(userName, "Username not found in header");
        Preconditions.checkNotNull(passwd, "Password not found in header");

        StrongPasswordEncryptor encryptor = new StrongPasswordEncryptor();
        return userMap.containsKey(userName) && encryptor.checkPassword(passwd, userMap.get(userName).encryptedPassword);
    }

    @Override
    public PravegaAccessControlEnum authorize(String resource, Map<String, String> headers) {
        String userName = headers.get("username");
        if (Strings.isNullOrEmpty(userName) || !userMap.containsKey(userName)) {
            throw new CompletionException(new PravegaAuthenticationException(userName));
        }
        return authorizeForUser(userMap.get(userName), resource);

    }

    private PravegaAccessControlEnum authorizeForUser(PravegaACls pravegaACls, String resource) {
        PravegaAccessControlEnum retVal = PravegaAccessControlEnum.NONE;

        for (PravegaAcl acl : pravegaACls.acls) {
            if (acl.resource.equals(resource)) {
                return acl.acl;
            } else if (resource.startsWith(acl.resource)) {
                retVal = acl.acl;
            } else if (acl.resource.equals("*")) {
                if (acl.acl.ordinal() > retVal.ordinal()) {
                    retVal = acl.acl;
                }
            }
        }
        return retVal;
    }

    private List<PravegaAcl> getAcls(String aclString) {
        return  Arrays.stream(aclString.split(";")).map(acl -> {
            String[] splits = acl.split(",");
            return new PravegaAcl(splits[0],
                    PravegaAccessControlEnum.valueOf(splits[1]));
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
        private final PravegaAccessControlEnum acl;


    }
}
