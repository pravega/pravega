/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework;

import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;

public class FailFastListener extends RunListener {

    private RunNotifier runNotifier;

    public FailFastListener(RunNotifier runNotifier) {
        super();
        this.runNotifier = runNotifier;
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
        this.runNotifier.pleaseStop();
    }
}
