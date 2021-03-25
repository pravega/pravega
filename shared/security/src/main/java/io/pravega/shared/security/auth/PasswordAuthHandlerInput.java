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
package io.pravega.shared.security.auth;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.Exceptions;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * This is a helper class for tests and may be used for generating the input file for the PasswordAuthHandler - the
 * default AuthHandler implementation.
 */
@Slf4j
public class PasswordAuthHandlerInput implements AutoCloseable {

    @Getter
    private File file;

    public PasswordAuthHandlerInput(String fileName, String extension) {
        Exceptions.checkNotNullOrEmpty(fileName, "fileName");
        Exceptions.checkNotNullOrEmpty(extension, "extension");
        try {
            file = File.createTempFile(fileName, extension);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void postEntry(Entry entry) {
        Preconditions.checkNotNull(entry, "Specified entry is null.");
        postEntries(Arrays.asList(entry));
    }

    public void postEntries(List<Entry> entries) {
        Exceptions.checkNotNullOrEmpty(entries, "entries");
        try (FileWriter writer = new FileWriter(file.getAbsolutePath())) {
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



    private String credentialsAndAclString(Entry entry) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(entry.username)
                && !Strings.isNullOrEmpty(entry.password)
                && entry.acl != null
                && !entry.acl.startsWith(":"));
        return String.format("%s:%s:%s%n", entry.username, entry.password, entry.acl);
    }

    @Override
    public void close() {
        try {
            this.file.delete();
        } catch (Exception e) {
            // Ignore
            log.warn("Unable to delete file", e);
        }
    }

    @Data(staticConstructor = "of")
    public static class Entry {

        private final String username;
        private final String password;
        private final String acl;
    }
}