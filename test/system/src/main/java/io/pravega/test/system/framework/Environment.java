/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * SystemTestRunner is a custom JUNIT test runner used for running system tests.
 * Before a system test is run, the required services (e.g: Pravega, Controller service, ZK services)
 * need to be specified along with its configuration.
 * A Static method annotated with @Environment is used to convey the the system configuration.
 * This method will invoke the test framework and deploy various services required by the System tests.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Environment {
}