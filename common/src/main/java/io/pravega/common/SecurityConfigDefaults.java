/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common;

/**
 * Holds default security configuration values.
 */
public class SecurityConfigDefaults {

    public static final String TLS_SERVER_CERT_PATH = "../config/server-cert.crt";

    public static final String TLS_SERVER_PRIVATE_KEY_PATH = "../config/server-key.key";

    public static final String TLS_SERVER_KEYSTORE_PATH = "../config/server.keystore.jks";

    public static final String TLS_CLIENT_TRUSTSTORE_PATH = "../config/client.truststore.jks";

    public static final String TLS_PASSWORD_PATH = "../config/server.keystore.jks.passwd";

    public static final String TLS_CA_CERT_PATH = "../config/ca-cert.crt";

    public static final String TLS_CA_PRIVATE_KEY_PATH = "../config/ca-key.key";

    public static final String AUTH_HANDLER_INPUT_PATH = "../config/passwd";

    public static final String AUTH_ADMIN_USERNAME = "admin";

    public static final String AUTH_ADMIN_PASSWORD = "1111_aaaa";

}
