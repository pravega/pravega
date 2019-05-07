/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.common;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Data;
import lombok.Getter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * This is a helper class for tests and may be used for generating the input file for the PasswordAuthHandler - the
 * default AuthHandler implementation.
 */
public class PasswordAuthHandlerInput {

    @Getter
    private File inputFile;

    public PasswordAuthHandlerInput() {
        this("auth_file", ".txt");
    }

    public PasswordAuthHandlerInput(String fileName, String extension) {
        try {
            inputFile = File.createTempFile(fileName, extension);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void postEntry(Entry entry) {
        postEntries(Arrays.asList(entry));
    }

    public void postEntries(List<Entry> entries) {
        try (FileWriter writer = new FileWriter(inputFile.getAbsolutePath())) {
            entries.forEach(e -> {
                try {
                    writer.write(credentialsAndAclString(e));
                } catch (IOException iE) {
                    throw new RuntimeException(iE);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String credentialsAndAclString (Entry entry) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(entry.username)
                && !Strings.isNullOrEmpty(entry.password)
                && entry.acl != null
                && !entry.acl.startsWith(":"));
        return String.format("%s:%s:%s%n", entry.username, entry.password, entry.acl);
    }

    @Data(staticConstructor="of")
    public static class Entry {

        private final String username;
        private final String password;
        private final String acl;
    }
}



