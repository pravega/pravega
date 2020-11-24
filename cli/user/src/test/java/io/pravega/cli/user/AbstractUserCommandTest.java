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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractUserCommandTest {

    @Rule
    public final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);
    // Setup utility.
    protected final SecureSetupUtils setupUtils = new SecureSetupUtils();
    protected final AtomicReference<InteractiveConfig> config = new AtomicReference<>();

    @Before
    public void setUp() throws Exception {
        setupUtils.startAllServices();
        InteractiveConfig interactiveConfig = InteractiveConfig.getDefault();
        interactiveConfig.setControllerUri(setupUtils.getControllerUri().toString().replace("tcp://", ""));
        interactiveConfig.setDefaultSegmentCount(4);
        interactiveConfig.setMaxListItems(100);
        interactiveConfig.setTimeoutMillis(1000);
        interactiveConfig.setAuthEnabled(setupUtils.isAuthEnabled());
        interactiveConfig.setUserName(SecurityConfigDefaults.AUTH_ADMIN_USERNAME);
        interactiveConfig.setPassword(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        interactiveConfig.setTlsEnabled(false);
        interactiveConfig.setTruststore("../../config/" + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME);
        config.set(interactiveConfig);
    }

    @After
    public void tearDown() throws Exception {
        setupUtils.close();
    }

}