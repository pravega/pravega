/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.framework;

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
     * Stop Test Execution given the testID.
     *  @param testID testIdentifier indicating the test to be terminated.
     *  @return a CompletableFuture which is completed once the test method execution is stopped.
     */
    CompletableFuture<Void> stopTestExecution(String testID);

}
