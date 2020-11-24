/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.auth;

/**
 * The configuration keys for auth plugin.
 */
public class AuthPluginConfig {

    /**
     * The path to the user account database file that the password-based Basic auth handler should use.
     */
    public static final String BASIC_AUTHPLUGIN_DATABASE = "basic.authplugin.dbfile";
}
