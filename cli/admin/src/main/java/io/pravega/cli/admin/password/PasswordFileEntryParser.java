/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.password;

import lombok.NonNull;

public class PasswordFileEntryParser {

    /**
     * Parses the specified credentials and ACL entry into an array containing a maximum of three elements.
     *
     * @param entry the credentials and ACL entry to be parsed
     * @return an array containing the parsed elements, typically a username, plaintext or hashed password and an ACL.
     */
    public static String[] parse(@NonNull String entry) {
        return parse(entry, true);
    }

    /**
     * Parses the specified credentials and ACL entry into an array containing a maximum of three elements.
     *
     * @param entry the credentials and ACL entry to be parsed
     * @param validateHasThreeElements whether to validate that the entry has exactly three elements. If this flag is
     *                                 true, and the entry does not contain three elements, this method will throw an
     *                                 an IllegalArgumentException.
     * @return an array containing the parsed elements, typically a username, plaintext or hashed password and an ACL.
     */
    public static String[] parse(@NonNull String entry, boolean validateHasThreeElements) {
        // A sample object value comprises of  "userName:password:acl". We don't want the splits at the
        // access control entries (like "prn::/scope:testScope") in the ACL, so we restrict the splits to 3.
        String[] result = entry.split(":", 3);

        if (validateHasThreeElements && result.length != 3) {
            throw new IllegalArgumentException(
                    "Entry does not contain exactly three elements of the form username:pwd:acl");
        }
        return result;
    }
}
