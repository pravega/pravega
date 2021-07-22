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
package io.pravega.test.system;

import io.pravega.test.system.framework.TestFrameworkException;
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

    private String methodName;
    private Class<?> testClass;

    /**
     * Creates a BlockJUnit4ClassRunner to run {@code testClass}.
     *  @param testClass Class containing the test to be run.
     *  @param method Name of the test method to be executed.
     *  @throws InitializationError if class is malformed.
     */
    public SingleJUnitTestRunner(Class<?> testClass, String method) throws InitializationError {
        super(testClass);
        this.testClass = testClass;
        this.methodName = method;
    }

    public void runMethod() {
        Method m = null;
        try {
            m = this.testClass.getDeclaredMethod(this.methodName);
            Statement statement = methodBlock(new FrameworkMethod(m) {
                @Override
                public Object invokeExplosively(final Object target, final Object... params) throws Throwable {
                    try {
                        Object result = super.invokeExplosively(target, params);
                        log.info("Test " + methodName + " completed without error.");
                        return result;
                    } catch (Throwable t) {
                        log.error("Test " + methodName + " failed with exception. ", t);
                        throw t;
                    }
                }
            });
            statement.evaluate();
        } catch (Throwable ex) {
            throw new TestFrameworkException(TestFrameworkException.Type.InternalError, "Exception while running test" +
                    " method: " + this.methodName, ex);
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

    public static void main(String... args) {
        String[] classAndMethod = args[0].split("#");
        //The return value is used to update the mesos task execution status. The mesos task is set to failed state when
        // return value is non-zero.
        System.exit(execute(classAndMethod[0], classAndMethod[1]) ? 0 : 1);
    }
}
