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
package com.emc.pravega.controller.task;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Method Annotation for marking methods as batch functions.
 * This annotation modifies the method in the following ways:
 * 1. It first tries to lock the path.
 * 2. If successful, it executes the method body, finally releasing the lock.
 * 3. If unsuccessful, it returns a Future with LockFailed exception.
 *
 * It uses instance two methods lock() and unlock(), if available in the class,
 * otherwise it generates definition for lock() and unlock() methods using a local variable $lock.
 *
 * Effectively, it replaces the method
 * Object method(Object... params) {
 *     body;
 * }
 *
 * with the following method
 *
 * Object method (Object... params) {
 *   try {
 *     CompletableFuture<Boolean> lock = this.lock();
 *     if (lock.get()) {
 *       body
 *     } else {
 *       throw new LockFailedException();
 *     }
 *   } finally {
 *     unlock();
 *   }
 * }
 */

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Task {

    public String name();
}
