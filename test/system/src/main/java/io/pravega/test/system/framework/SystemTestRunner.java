/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.system.framework;

import io.pravega.common.Exceptions;
import io.pravega.test.system.framework.TestExecutorFactory.TestExecutorType;
import lombok.extern.slf4j.Slf4j;
import org.junit.internal.runners.model.EachTestNotifier;
import org.junit.internal.runners.statements.RunBefores;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.pravega.common.Exceptions.unwrap;
import static io.pravega.test.system.framework.Utils.getConfig;

/**
 * SystemTestRunner this is used to execute all the systemTests.
 */
@Slf4j
public class SystemTestRunner extends BlockJUnit4ClassRunner {

    /**
     * Creates a BlockJUnit4ClassRunner to run {@code testClass}.
     *
     *  @param testClass class to be tested.
     *  @throws InitializationError if the test class is malformed.
     */
    public SystemTestRunner(Class<?> testClass) throws InitializationError {
        super(testClass);
    }

    @Override
    protected Statement classBlock(final RunNotifier notifier) {
        final Statement statement = super.classBlock(notifier);
        if (Utils.isSkipServiceInstallationEnabled()) {
            log.info("skipServiceInstallation flag is set, skipping invocation of @Environment method");
            return statement;
        } else {
            log.info("skipServiceInstallation flag is disabled, invoking @Environment method");
            return withEnvironment(statement);
        }
    }

    @Override
    protected void runChild(final FrameworkMethod method, RunNotifier notifier) {
        Description description = describeChild(method);
        if (isIgnored(method)) {
            notifier.fireTestIgnored(description);
        } else {
            //read the type of testExecutor from system property. This is sent by the gradle task. By default
            //the tests are executed locally.
            TestExecutorType executionType = TestExecutorType.valueOf(getConfig("execType", "LOCAL"));
            //sleep for 15 seconds before running tests, remove once pravega/pravega/issues/1665 is resolved
            Exceptions.handleInterrupted(() -> TimeUnit.SECONDS.sleep(15));
            invokeTest(notifier, executionType, method);
        }
    }

    private CompletableFuture<Void> execute(TestExecutorType type, Method method) throws Exception {
        return new TestExecutorFactory().getTestExecutor(type).startTestExecution(method);
    }

    private void invokeTest(RunNotifier notifier, TestExecutorType type, FrameworkMethod method) {
        if ((type == null) || TestExecutorType.LOCAL.equals(type)) {
            runLeaf(methodBlock(method), describeChild(method), notifier);
        } else {
            EachTestNotifier eachNotifier = new EachTestNotifier(notifier, describeChild(method));
            try {
                eachNotifier.fireTestStarted();
                execute(type, method.getMethod()).get();
            } catch (Throwable e) {
                log.error("Test " + method + " failed with exception ", e);
                eachNotifier.addFailure(unwrap(e));
            } finally {
                eachNotifier.fireTestFinished();
            }
        }
    }

    private Statement withEnvironment(Statement statement) {
        List<FrameworkMethod> environment = super.getTestClass().getAnnotatedMethods(Environment.class);
        if (environment.isEmpty()) {
            log.error("@Environment annotation not used for system test , {}", getTestClass().getName());
            return statement;
        } else {
            return new RunBefores(statement, environment, null);
        }
    }
}
