/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega;

import com.emc.pravega.framework.TestFrameworkException;
import lombok.extern.slf4j.Slf4j;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.lang.reflect.Method;

/**
 * Helper runner used to run an individual test case.
 */
@Slf4j
public class SingleJUnitTestRunner extends BlockJUnit4ClassRunner {
    /**
     * Creates a BlockJUnit4ClassRunner to run {@code klass}.
     *
     * @param klass
     * @throws InitializationError if the test class is malformed.
     */
    private String methodName;
    private Class<?> className;

    public SingleJUnitTestRunner(Class<?> klass, String method) throws InitializationError {
        super(klass);
        className = klass;
        methodName = method;

    }

    public void runMethod() {
        Method m = null;
        try {
            m = className.getDeclaredMethod(methodName);
            Statement statement = methodBlock(new FrameworkMethod(m));
            statement.evaluate();
        } catch (Throwable ex) {
            throw new TestFrameworkException(TestFrameworkException.Type.InternalError, "Exception while running test" +
                    " method: " + methodName, ex);
        }
    }

    public static boolean execute(String className, String methodName) {
        try {
            SingleJUnitTestRunner runner = new SingleJUnitTestRunner(Class.forName(className), methodName);
            runner.runMethod();
            return true;
        } catch (Throwable ex) {
            log.error("Error while executing the test", ex);
            return false;
        }
    }

    public static void main(String... args) throws ClassNotFoundException {
        String[] classAndMethod = args[0].split("#");
        //The return value is used to update the mesos task execution status. The mesos task is set to failed state when
        // return value is non-zero.
        System.exit(execute(classAndMethod[0], classAndMethod[1]) ? 0 : 1);
    }
}
