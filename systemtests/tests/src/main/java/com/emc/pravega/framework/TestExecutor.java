/**
 * Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
