/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin;

import io.pravega.test.integration.utils.SecureSetupUtils;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.cli.admin.utils.TestUtils.setAdminCLIProperties;

public abstract class AbstractAdminCommandTest {

    @Rule
    public final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);
    // Setup utility.
    protected SecureSetupUtils setupUtils;
    protected boolean authEnabled = false;
    protected final AtomicReference<AdminCommandState> state = new AtomicReference<>();

    @Before
    @SneakyThrows
    public void setUp() {
        setupUtils = new SecureSetupUtils(authEnabled);
        setupUtils.startAllServices();
        setAdminCLIProperties(setupUtils.getControllerRestUri().toString(),
                setupUtils.getControllerUri().toString(),
                setupUtils.getZkTestServer().getConnectString(),
                4, authEnabled, "secret", false, state);
    }

    @After
    @SneakyThrows
    public void tearDown() {
        setupUtils.close();
        state.get().close();
    }
}
