/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user;


import io.pravega.cli.user.config.InteractiveConfig;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.demo.ClusterWrapper;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class to contain convenient utilities for writing test cases.
 */
public final class TestUtils {

    /**
     * Invoke any command and get the result by using a mock PrintStream object (instead of System.out). The returned
     * String is the output written by the Command that can be check in any test.
     *
     * @param inputCommand Command to execute.
     * @param config       Configuration to execute the command.
     * @return             Output of the command.
     * @throws Exception   If a problem occurs.
     */
    public static String executeCommand(String inputCommand, InteractiveConfig config) throws Exception {
        Parser.Command pc = Parser.parse(inputCommand, config);
        CommandArgs args = new CommandArgs(pc.getArgs().getArgs(), config);
        Command cmd = Command.Factory.get(pc.getComponent(), pc.getName(), args);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8)) {
            assert cmd != null;
            cmd.setOut(ps);
            cmd.execute();
        }
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    /**
     * Returns the relative path to `pravega/config` source directory from cli tests.
     *
     * @return the path
     */
    public static String pathToConfig() {
        return "../../config/";
    }

    public static ClusterWrapper createPravegaCluster(boolean authEnabled, boolean tlsEnabled) {
        return ClusterWrapper.builder()
                .authEnabled(authEnabled)
                .tlsEnabled(tlsEnabled)
                .tlsServerCertificatePath(pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                .tlsServerKeyPath(pathToConfig() + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME)
                .tlsHostVerificationEnabled(false)
                .build();
    }

    public static void setInteractiveConfig(String controllerUri, boolean authEnabled, boolean tlsEnabled,
                                            AtomicReference<InteractiveConfig> config) {
        InteractiveConfig interactiveConfig = InteractiveConfig.getDefault();
        interactiveConfig.setControllerUri(controllerUri);
        interactiveConfig.setDefaultSegmentCount(4);
        interactiveConfig.setMaxListItems(100);
        interactiveConfig.setTimeoutMillis(10000);
        interactiveConfig.setAuthEnabled(authEnabled);
        interactiveConfig.setUserName(SecurityConfigDefaults.AUTH_ADMIN_USERNAME);
        interactiveConfig.setPassword(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        interactiveConfig.setTlsEnabled(tlsEnabled);
        interactiveConfig.setTruststore(pathToConfig() + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME);
        config.set(interactiveConfig);
    }
}