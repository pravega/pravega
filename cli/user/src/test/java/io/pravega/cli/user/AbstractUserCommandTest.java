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
import io.pravega.test.integration.utils.SecureSetupUtils;
import org.junit.*;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractUserCommandTest {

    // Setup utility.
    protected static final SecureSetupUtils SETUP_UTILS = new SecureSetupUtils();
    protected static final AtomicReference<InteractiveConfig> CONFIG = new AtomicReference<>();

    @Rule
    public final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    @Before
    public void setUp() throws Exception {
        SETUP_UTILS.startAllServices();
        InteractiveConfig interactiveConfig = InteractiveConfig.getDefault();
        interactiveConfig.setControllerUri(SETUP_UTILS.getControllerUri().toString());
        interactiveConfig.setDefaultSegmentCount(4);
        interactiveConfig.setMaxListItems(100);
        interactiveConfig.setTimeoutMillis(1000);
        interactiveConfig.setAuthEnabled(SETUP_UTILS.isAuthEnabled());
        interactiveConfig.setUserName(SecurityConfigDefaults.AUTH_ADMIN_USERNAME);
        interactiveConfig.setPassword(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        interactiveConfig.setTlsEnabled(false);
        interactiveConfig.setTruststore("../../config/" + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME);
        CONFIG.set(interactiveConfig);
    }

    @After
    public void tearDown() throws Exception {
        SETUP_UTILS.close();
    }

}