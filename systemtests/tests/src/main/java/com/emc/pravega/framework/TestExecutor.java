/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.framework;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

/**
 * Test Executor interface.
 */
public interface TestExecutor {

    /**
     * Start Test Execution given a Method.
     * @param method java.lang.reflect.Method test method to be executed.
     * @return
     */
    CompletableFuture<Void> startTestExecution(Method method);

    /**
     * Stop Test Execution given the testID.
     * @param testID testIdentifier indicating the test to be terminated.
     * @return
     */
    CompletableFuture<Void> stopTestExecution(String testID);

}
