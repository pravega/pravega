/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega;

import lombok.extern.slf4j.Slf4j;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.lang.reflect.Method;

@Slf4j
public class SingleJUnitTestRunner extends BlockJUnit4ClassRunner {
    /**
     * Creates a BlockJUnit4ClassRunner to run {@code klass}
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

    public void runMethod() throws Throwable {
        Method m = className.getDeclaredMethod(methodName);
        Statement statement = methodBlock(new FrameworkMethod(m));
        statement.evaluate();
    }

    public static boolean execute(String className, String methodName) {
        try {
            SingleJUnitTestRunner runner = new SingleJUnitTestRunner(Class.forName(className), methodName);
            runner.runMethod();
            return false;
        } catch (Throwable ex) {
            log.error("Error while executing the test", ex);
            return true;
        }
    }

    public static void main(String... args) throws ClassNotFoundException {
        String[] classAndMethod = args[0].split("#");
        //Request request = Request.method(Class.forName(classAndMethod[0]),
        //classAndMethod[1]);

        // Result result = new JUnitCore().run(request);
        // return 0 in case the execution is successful.
        // return 1 in case the execution is a failure.
        System.exit(execute(classAndMethod[0], classAndMethod[1]) ? 0 : 1);
    }
}