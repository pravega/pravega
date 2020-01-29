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

public class AuthFileUtils {

    public static String credentialsAndAclAsString(String username, String password, String acl) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(username)
                && !Strings.isNullOrEmpty(password)
                && acl != null
                && !acl.startsWith(":"));

        // This will return a string that looks like this:"<username>:<pasword>:acl\n"
        return String.format("%s:%s:%s%n", username, password, acl);
    }
}
