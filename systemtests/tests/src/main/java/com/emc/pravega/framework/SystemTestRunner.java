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
package com.emc.pravega.framework;

import com.emc.pravega.SingleJUnitTestRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.internal.runners.statements.RunBefores;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.lang.reflect.Method;
import java.util.List;

/**
 * SystemTestRunner this is used to execute all the systemTests.
 */
@Slf4j
public class SystemTestRunner extends BlockJUnit4ClassRunner {

    /**
     * Creates a BlockJUnit4ClassRunner to run {@code klass}.
     *
     * @param klass class to be tested.
     * @throws InitializationError if the test class is malformed.
     */
    public SystemTestRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    protected Statement classBlock(final RunNotifier notifier) {
        final Statement statement = super.classBlock(notifier);
        return withEnvironment(statement);
    }


    @Override
    protected void runChild(final FrameworkMethod method, RunNotifier notifier) {
        Description description = describeChild(method);
        if (isIgnored(method)) {
            notifier.fireTestIgnored(description);
        } else {
            Method m1 = method.getMethod();
            //Execute method ; currently it is directly being executed by SingleJUnitTestRunner
            //this can be executed using marathon too.
            //TODO: Set usage using marathon.
            SingleJUnitTestRunner.execute(m1.getDeclaringClass().getName(), m1.getName());
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
