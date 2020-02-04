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

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

/**
 * Test Executor interface.
 */
public interface TestExecutor {

    /**
     * Start Test Execution given a test method.
     *  @param testMethod test method to be executed.
     *  @return a CompletableFuture which is completed once the test method execution is completed.
     */
    CompletableFuture<Void> startTestExecution(Method testMethod);

    /**
     * Stop Test Execution.
     *
     */
    void stopTestExecution();

}
