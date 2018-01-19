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

import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import org.jasypt.util.password.StrongPasswordEncryptor;

public class PasswordFileCreatorTool {
    public static void main(String[] args) throws IOException {
        String fileName = args[0];

        StrongPasswordEncryptor passwordEncryptor = new StrongPasswordEncryptor();
        try (FileWriter writer = new FileWriter(fileName);
             BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                String s = bufferRead.readLine();
                if (Strings.isNullOrEmpty(s)) {
                    break;
                }
                String[] lists = s.split(":");
                String toWrite = lists[0] + ":" + passwordEncryptor.encryptPassword(lists[1])
                        + ":" + lists[2];
                writer.write(toWrite + "\n");
                writer.flush();
            }
        }
    }
}
