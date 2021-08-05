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
package io.pravega.test.common;

/**
 * Holds default security configuration values.
 */
public class SecurityConfigDefaults {
    public static final String[] TLS_PROTOCOL_VERSION = new String[]{ "TLSv1.2", "TLSv1.3"};

    public static final String TLS_SERVER_CERT_FILE_NAME = "server-cert.crt";

    public static final String TLS_SERVER_CERT_PATH = "../config/" + TLS_SERVER_CERT_FILE_NAME;

    public static final String TLS_SERVER_PRIVATE_KEY_FILE_NAME = "server-key.key";

    public static final String TLS_SERVER_PRIVATE_KEY_PATH = "../config/" + TLS_SERVER_PRIVATE_KEY_FILE_NAME;

    public static final String TLS_SERVER_KEYSTORE_NAME = "server.keystore.jks";

    public static final String TLS_SERVER_KEYSTORE_PATH = "../config/" + TLS_SERVER_KEYSTORE_NAME;

    public static final String TLS_CLIENT_TRUSTSTORE_PATH = "../config/client.truststore.jks";

    public static final String TLS_CLIENT_TRUSTSTORE_NAME = "client.truststore.jks";

    public static final String TLS_PASSWORD_FILE_NAME = "server.keystore.jks.passwd";

    public static final String TLS_PASSWORD_PATH = "../config/" + TLS_PASSWORD_FILE_NAME;

    public static final String TLS_CA_CERT_FILE_NAME = "ca-cert.crt";

    public static final String TLS_CA_CERT_PATH = "../config/" + TLS_CA_CERT_FILE_NAME;

    public static final String AUTH_HANDLER_INPUT_FILE_NAME = "passwd";

    public static final String AUTH_HANDLER_INPUT_PATH = "../config/" + AUTH_HANDLER_INPUT_FILE_NAME;

    public static final String AUTH_ADMIN_USERNAME = "admin";

    public static final String AUTH_ADMIN_PASSWORD = "1111_aaaa";

    public static final String TLS_BK_KEYSTORE_FILE_NAME = "bookie.keystore.jks";

    public static final String TLS_BK_KEYSTORE_PASSWORD_FILE_NAME = "bookie.keystore.jks.passwd";

    public static final String TLS_BK_TRUSTSTORE_FILE_NAME = "bookie.truststore.jks";
}
