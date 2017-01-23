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

import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

//@Distributed -- Distributed tests: All the tests are executed as marathon jobs.
//@Simple -- Each test is executed (as a marathon job ) one after another.
//@Local -- Each test is executed locally and not as a marathon job.
//@FreshSetup -- this is used to indicate the Setup needs to be created afresh.

@RunWith(SystemTestRunner.class)
public class PrimeNumberCheckerTest {
    private PrimeNumberChecker primeNumberChecker = new PrimeNumberChecker();

    @Environment
    public static void requiredEnvironment() {
        System.out.println("Environment");
    }

    @BeforeClass
    public static void beforeEveryone() {
        System.out.println("BeforeClass invoked");
    }

    @Before
    public void before() {
        System.out.println("Before every test method invoked");
    }

    @Test
    public void validate() throws Exception {
        System.out.println("Invoking validate() test");
        int inputNumber = Integer.valueOf(System.getProperty("number", "13"));
        System.out.println("Parameterized Number is : " + inputNumber);
        assertEquals(true, primeNumberChecker.validate(inputNumber));
    }

    @After
    public void after() {

    }

    @Test
    public void validate2() {
        System.out.println("Invoking validate2() test");
        assertTrue("Testing a failure condition", false);
    }

}