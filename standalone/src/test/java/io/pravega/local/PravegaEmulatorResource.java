/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;

import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.TestUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.rules.ExternalResource;

/**
 * This class contains the rules to start and stop LocalPravega / standalone.
 * This resource can be configured for a test using @ClassRule and it will ensure standalone is started once
 * for all the methods in a test and is shutdown once all the tests methods have completed execution.
 *
 * - Usage pattern to start it once for all @Test methods in a class
 *  <pre>
 *  &#64;ClassRule
 *  public static final PravegaEmulatorResource EMULATOR = new PravegaEmulatorResource(false, false, false);
 *  </pre>
 *  - Usage pattern to start it before every @Test method.
 *  <pre>
 *  &#64;Rule
 *  public final PravegaEmulatorResource emulator = new PravegaEmulatorResource(false, false, false);
 *  </pre>
 *
 */
@Slf4j
public class PravegaEmulatorResource extends ExternalResource {
    final boolean authEnabled;
    final boolean tlsEnabled;
    final LocalPravegaEmulator pravega;

    /**
     * Create an instance of Pravega Emulator resource.
     * @param authEnabled Authorisation enable flag.
     * @param tlsEnabled  Tls enable flag.
     * @param restEnabled REST endpoint enable flag.
     */
    public PravegaEmulatorResource(boolean authEnabled, boolean tlsEnabled, boolean restEnabled) {
        this.authEnabled = authEnabled;
        this.tlsEnabled = tlsEnabled;
        LocalPravegaEmulator.LocalPravegaEmulatorBuilder emulatorBuilder = LocalPravegaEmulator.builder()
                .controllerPort(TestUtils.getAvailableListenPort())
                .segmentStorePort(TestUtils.getAvailableListenPort())
                .zkPort(TestUtils.getAvailableListenPort())
                .restServerPort(TestUtils.getAvailableListenPort())
                .enableRestServer(restEnabled)
                .enableAuth(authEnabled)
                .enableTls(tlsEnabled);

        // Since the server is being built right here, avoiding delegating these conditions to subclasses via factory
        // methods. This is so that it is easy to see the difference in server configs all in one place. This is also
        // unlike the ClientConfig preparation which is being delegated to factory methods to make their preparation
        // explicit in the respective test classes.

        if (authEnabled) {
            emulatorBuilder.passwdFile(SecurityConfigDefaults.AUTH_HANDLER_INPUT_PATH)
                    .userName(SecurityConfigDefaults.AUTH_ADMIN_USERNAME)
                    .passwd(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        }
        if (tlsEnabled) {
            emulatorBuilder.certFile(SecurityConfigDefaults.TLS_SERVER_CERT_PATH)
                    .keyFile(SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_PATH)
                    .jksKeyFile(SecurityConfigDefaults.TLS_SERVER_KEYSTORE_PATH)
                    .jksTrustFile(SecurityConfigDefaults.TLS_CLIENT_TRUSTSTORE_PATH)
                    .keyPasswordFile(SecurityConfigDefaults.TLS_PASSWORD_PATH);
        }

        pravega = emulatorBuilder.build();
    }

    protected void before() throws Exception {
        pravega.start();
    }

    @SneakyThrows
    protected void after() {
        pravega.close();
    }
}


