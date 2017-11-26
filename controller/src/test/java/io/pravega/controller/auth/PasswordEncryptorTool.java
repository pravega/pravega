/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.auth;

import org.jasypt.util.password.StrongPasswordEncryptor;

public class PasswordEncryptorTool {

    public static void main(String[] args) {
        String userName = args[0];
        String userPassword = args[1];
        StrongPasswordEncryptor passwordEncryptor = new StrongPasswordEncryptor();
        String encryptedPassword = passwordEncryptor.encryptPassword(userPassword);
        System.out.println(encryptedPassword);
    }
}
