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
package io.pravega.auth;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.FileWriter;
import java.util.List;
import java.util.stream.Collectors;

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
        return aceStrings.stream().collect(Collectors.joining(";"));
    }

    @SneakyThrows
    public static void addAuthFileEntry(@NonNull FileWriter writer, @NonNull String entryAsString) {
        writer.write(entryAsString + "\n");
    }

    @SneakyThrows
    public static void addAuthFileEntry(@NonNull FileWriter writer, @NonNull String username,
                                           @NonNull String hashedPwd, @NonNull List<String> aceStrings) {
        String fileEntry = String.format("%s:%s:%s", username, hashedPwd, createAclString(aceStrings));
        addAuthFileEntry(writer, fileEntry);
    }
}
