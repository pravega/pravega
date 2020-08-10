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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.FileWriter;
import java.util.List;

public class AuthFileUtils {

    public static String credentialsAndAclAsString(String username, String password, String acl) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(username)
                && !Strings.isNullOrEmpty(password)
                && acl != null
                && !acl.startsWith(":"));

        // This will return a string that looks like this:"<username>:<pasword>:acl\n"
        return String.format("%s:%s:%s%n", username, password, acl);
    }

    public static String createAclString(@NonNull List<String> aceStrings) {
        StringBuilder builder = new StringBuilder();
        for (String aceString: aceStrings) {
            builder.append(aceString).append(";");
        }
        return builder.toString();
    }

    @SneakyThrows
    public static void addAuthFileEntry(@NonNull FileWriter writer, @NonNull String entryAsString) {
        StringBuilder builder = new StringBuilder(entryAsString);
        writer.write(builder.append("\n").toString());
    }

    @SneakyThrows
    public static void addAuthFileEntry(@NonNull FileWriter writer, @NonNull String username,
                                           @NonNull String hashedPwd, @NonNull List<String> aceStrings) {
        StringBuilder builder = new StringBuilder();
        builder.append(username).append(":").append(hashedPwd).append(":").append(createAclString(aceStrings));
        addAuthFileEntry(writer, builder.toString());
    }
}
