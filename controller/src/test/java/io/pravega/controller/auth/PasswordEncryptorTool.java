/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.auth;

import io.pravega.controller.server.rpc.auth.StrongPasswordProcessor;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

public class PasswordEncryptorTool {

    public static void main(String[] args) throws InvalidKeySpecException, NoSuchAlgorithmException {
        String userName = args[0];
        String userPassword = args[1];
        StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();
        String encryptedPassword = passwordEncryptor.encryptPassword(userPassword);
        System.out.println(encryptedPassword);
    }
}
