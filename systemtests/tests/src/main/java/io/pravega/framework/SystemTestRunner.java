/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.framework;

import io.pravega.framework.TestExecutorFactory.TestExecutorType;
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
            TestExecutorType executionType = TestExecutorType.valueOf(System.getProperty("execType", "LOCAL"));
            invokeTest(notifier, executionType, method);
        }
    }

    private CompletableFuture<Void> execute(TestExecutorType type, Method method) throws Exception {
        return TestExecutorFactory.getTestExecutor(type).startTestExecution(method);
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
                eachNotifier.addFailure(e);
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
